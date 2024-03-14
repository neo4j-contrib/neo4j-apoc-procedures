package apoc.couchbase;

import apoc.util.TestUtil;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CouchbaseIT {

    private static int numberConnections = 0;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        CouchbaseTestUtils.createCouchbaseContainer();

        Map<String, Object> properties = Map.of(
                CouchbaseTestUtils.BASE_CONFIG_KEY + CouchbaseManager.URI_CONFIG_KEY, "localhost",
                CouchbaseTestUtils.BASE_CONFIG_KEY + CouchbaseManager.USERNAME_CONFIG_KEY, CouchbaseTestUtils.USERNAME,
                CouchbaseTestUtils.BASE_CONFIG_KEY + CouchbaseManager.PASSWORD_CONFIG_KEY, CouchbaseTestUtils.PASSWORD
        );
        properties.forEach((key, value) -> apocConfig().setProperty(key, value));

        TestUtil.registerProcedure(db, Couchbase.class);
    }

    @AfterClass
    public static void tearDown() {
        if (CouchbaseTestUtils.couchbase != null) {
            CouchbaseTestUtils.couchbase.stop();
        }
        db.shutdown();
    }
    
    @Before
    public void before() {
        numberConnections = CouchbaseTestUtils.getNumConnections();
    }

    @After
    public void after() {
        // the connections active before must be equal to the connections active after
        assertEventually(() -> CouchbaseTestUtils.getNumConnections(), value -> value <= numberConnections, 60L, TimeUnit.SECONDS);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetViaCall() {
        testCall(db, "CALL apoc.couchbase.get($host, $bucket, 'artist:vincent_van_gogh')",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME),
                r -> {
                    assertTrue(r.get("content") instanceof Map);
                    Map<String, Object> content = (Map<String, Object>) r.get("content");
                    assertTrue(content.get("notableWorks") instanceof List);
                    List<String> notableWorks = (List<String>) content.get("notableWorks");
                    CouchbaseTestUtils.checkDocumentContent(
                            (String) content.get("firstName"),
                            (String) content.get("secondName"),
                            (String) content.get("lastName"),
                            notableWorks);
                });
    }

    @Test
    public void testUpsertWithMutationTokenDisabled() {
        // with mutationTokensEnabledv: false we expect that "mutationToken" in the result row should be null
        testCall(db, "CALL apoc.couchbase.upsert($host, $bucket, 'testUpsertViaCall', $data, $config)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", CouchbaseTestUtils.VINCENT_VAN_GOGH.toString(),
                        "config", map("mutationTokensEnabled", false)),
                r -> {
                    // this should be null
                    assertNull(r.get("mutationToken"));
                    assertTrue(r.get("content") instanceof Map);
                    Map<String, Object> content = (Map<String, Object>) r.get("content");
                    assertTrue(content.get("notableWorks") instanceof List);
                    List<String> notableWorks = (List<String>) content.get("notableWorks");
                    CouchbaseTestUtils.checkDocumentContent(
                            (String) content.get("firstName"),
                            (String) content.get("secondName"),
                            (String) content.get("lastName"),
                            notableWorks);
                    CouchbaseTestUtils.collection.remove("testUpsertViaCall");
                    assertFalse(CouchbaseTestUtils.collection.exists("testUpsertViaCall").exists());
                });
    }

    @Test
    public void testGetWithCustomCollection() {
        // with config collection: "<COLLECTION_NAME>" we should get only results coming from 
        //  com.couchbase.client.java.manager.collection.CollectionManager.createCollection("<COLLECTION_NAME>")
        //  instead of default collection ("_default");
        testCall(db, "CALL apoc.couchbase.get($host, $bucket, $documentId, $config)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "documentId", "foo:bar", "config", map("collection", CouchbaseTestUtils.COLL_NAME)),
                r -> {
                    Map<String, Object> content = (Map<String, Object>) r.get("content");
                    assertEquals("beta", content.get("alpha"));
                });
    }
    
    @Test
    public void testGetWithCustomScope() {
        // with config scope: "<SCOPE_NAME>" and collection: "<COLLECTION_NAME>" we should get only results coming from 
        //  com.couchbase.client.java.manager.collection.CollectionManager.createScope("<SCOPE_NAME>") and CollectionManager.createCollection("<COLLECTION_NAME>")
        //  instead of default collection and scope (both "_default");
        testCall(db, "CALL apoc.couchbase.get($host, $bucket, $documentId, $config)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "documentId", "secondScope", 
                        "config", map("collection", CouchbaseTestUtils.SECOND_COLL_NAME, "scope", CouchbaseTestUtils.SECOND_SCOPE)),
                r -> {
                    Map<String, Object> content = (Map<String, Object>) r.get("content");
                    assertEquals("two", content.get("one"));
                });
    }

    @Test
    public void testExistsViaCallEmptyResult() {
        testCallEmpty(db, "CALL apoc.couchbase.get($host, $bucket, 'notExists')",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME));
    }

    @Test
    public void testExistsViaCall() {
        testCall(db, "CALL apoc.couchbase.exists($host, $bucket, 'artist:vincent_van_gogh')",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME),
                r -> assertTrue((boolean) r.get("value")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsertViaCall() {
        testCall(db, "CALL apoc.couchbase.insert($host, $bucket, 'testInsertViaCall', $data)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", CouchbaseTestUtils.VINCENT_VAN_GOGH.toString()),
                r -> {
                    assertTrue(r.get("content") instanceof Map);
                    Map<String, Object> content = (Map<String, Object>) r.get("content");
                    assertTrue(content.get("notableWorks") instanceof List);
                    List<String> notableWorks = (List<String>) content.get("notableWorks");
                    CouchbaseTestUtils.checkDocumentContent(
                            (String) content.get("firstName"),
                            (String) content.get("secondName"),
                            (String) content.get("lastName"),
                            notableWorks);
                    CouchbaseTestUtils.collection.remove("testInsertViaCall");
                    assertFalse(CouchbaseTestUtils.collection.exists("testInsertViaCall").exists());
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithConnectTimeout() {
        try {
            testCall(db, "CALL apoc.couchbase.insert($host, $bucket, 'testConnectTimeout', $data, $config)",
                    map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", CouchbaseTestUtils.BIG_JSON.toString(),
                            "config", map("connectTimeout", 1)),
                r -> fail("Should fail because of AmbiguousTimeoutException"));
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof AmbiguousTimeoutException);
            assertTrue("should throw a timeout message", rootCause.getMessage().startsWith("InsertRequest, Reason: TIMEOUT"));
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithKvTimeout() {
        try {
            testCall(db, "CALL apoc.couchbase.insert($host, $bucket, 'testKvTimeout', $data, $config)", 
                    map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", CouchbaseTestUtils.BIG_JSON.toString(),
                        "config", map("kvTimeout", 1)),
                r -> fail("Should fail because of AmbiguousTimeoutException"));
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof AmbiguousTimeoutException);
            assertTrue("should throw a timeout message", rootCause.getMessage().startsWith("InsertRequest, Reason: TIMEOUT"));
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithWaitUntilReadyTimeout() {
        try {
            testCall(db, "CALL apoc.couchbase.insert($host, $bucket, 'testWaitUntilReady', $data, $config)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", CouchbaseTestUtils.BIG_JSON.toString(),
                        "config", map("waitUntilReady", 1)),
                r -> fail("Should fail because of UnambiguousTimeoutException"));
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof UnambiguousTimeoutException);
            assertTrue("should throw a timeout message", rootCause.getMessage().startsWith("WaitUntilReady timed out"));
            throw e;
        }
    }

    @Test
    public void testAppendViaCall() {
        String input = "hello";
        byte[] bytes = input.getBytes();

        final String expectedId = "binaryId";
        CouchbaseTestUtils.collection.insert(expectedId, bytes, InsertOptions.insertOptions().transcoder(RawBinaryTranscoder.INSTANCE));

        testCall(db, "CALL apoc.couchbase.append($host, $bucket, 'binaryId', $data, $config)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", " {from: 'world'}".getBytes(),
                        "config", map()),
                r -> {
                    final String actualContent = new String((byte[]) r.get("content"));
                    final String actualId = (String) r.get("id");
                    assertEquals(expectedId, actualId);
                    assertEquals("hello {from: 'world'}", actualContent);
                    CouchbaseTestUtils.collection.remove(expectedId);
                    assertFalse(CouchbaseTestUtils.collection.exists(expectedId).exists());
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testUpsertFailBecauseOfIncorrectTranscoder() {
        try {
            testCall(db, "CALL apoc.couchbase.upsert($host, $bucket, 'testUpsertViaCall', $data, {transcoder: 'rawbinary'})",
                    map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", CouchbaseTestUtils.VINCENT_VAN_GOGH.toString()),
                    r -> fail("Should fail because of rawbinary wrong config"));
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof CouchbaseException);
            assertEquals("Only byte[] is supported for the RawBinaryTranscoder!", rootCause.getMessage());
            throw e;
        }
    }

    @Test
    public void testPrependViaCall() {
        String input = " world";
        byte[] bytes = input.getBytes();

        final String expectedId = "binaryId";
        CouchbaseTestUtils.collection.insert(expectedId, bytes, InsertOptions.insertOptions().transcoder(RawBinaryTranscoder.INSTANCE));
        numberConnections = CouchbaseTestUtils.getNumConnections();

        testCall(db, "CALL apoc.couchbase.prepend($host, $bucket, 'binaryId', $data)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", "hello".getBytes()),
                r -> {
                    final String actualContent = new String((byte[]) r.get("content"));
                    final String actualId = (String) r.get("id");
                    assertEquals(expectedId, actualId);
                    assertEquals("hello world", actualContent);
                    CouchbaseTestUtils.collection.remove(expectedId);
                    assertFalse(CouchbaseTestUtils.collection.exists(expectedId).exists());
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testInsertWithAlreadyExistingIDViaCall() {
        try {
            testCall(db, "CALL apoc.couchbase.insert($host, $bucket, 'artist:vincent_van_gogh', $data)",
                    map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", CouchbaseTestUtils.VINCENT_VAN_GOGH.toString()),
                    r -> {});
        } catch (QueryExecutionException e) {
            assertTrue(ExceptionUtils.getRootCause(e) instanceof DocumentExistsException);
            throw e;
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpsertViaCall() {
        testCall(db, "CALL apoc.couchbase.upsert($host, $bucket, 'testUpsertViaCall', $data)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "data", CouchbaseTestUtils.VINCENT_VAN_GOGH.toString()),
                r -> {
                    assertTrue(r.get("mutationToken") instanceof Map);
                    assertTrue(r.get("content") instanceof Map);
                    Map<String, Object> content = (Map<String, Object>) r.get("content");
                    assertTrue(content.get("notableWorks") instanceof List);
                    List<String> notableWorks = (List<String>) content.get("notableWorks");
                    CouchbaseTestUtils.checkDocumentContent(
                            (String) content.get("firstName"),
                            (String) content.get("secondName"),
                            (String) content.get("lastName"),
                            notableWorks);
                    CouchbaseTestUtils.collection.remove("testUpsertViaCall");
                    assertFalse(CouchbaseTestUtils.collection.exists("testUpsertViaCall").exists());
                });
    }

    @Test
    public void testRemoveViaCall() {
        CouchbaseTestUtils.collection.insert("testRemove", JsonObject.create());
        testCall(db, "CALL apoc.couchbase.remove($host, $bucket, 'testRemove')",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME),
                r -> assertFalse(CouchbaseTestUtils.collection.exists("testRemove").exists()));
    }

    @Test
    public void testQueryViaCall() {
        testCall(db, "CALL apoc.couchbase.query($host, $bucket, $query)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "query", "select * from " + CouchbaseTestUtils.BUCKET_NAME + " where lastName = \"Van Gogh\""),
                r -> CouchbaseTestUtils.checkListResult(r));
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithQueryTimeout() {
        try {
            testCall(db, "CALL apoc.couchbase.query($host, $bucket, $query, $config)",
                    map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "query", "select * from " + CouchbaseTestUtils.BUCKET_NAME + " where lastName = \"Van Gogh\"",
                            "config", map("queryTimeout", 1)),
                    r -> fail("Should fail because of AmbiguousTimeoutException"));
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof AmbiguousTimeoutException);
            assertTrue("should throw a timeout message", rootCause.getMessage().startsWith("QueryRequest, Reason: TIMEOUT"));
            throw e;
        }
    }

    @Test
    public void testQueryViaCallEmptyResult() {
        testCallEmpty(db, "CALL apoc.couchbase.query($host, $bucket, $query)",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "query", "select * from " + CouchbaseTestUtils.BUCKET_NAME + " where lastName = 'notExists'"));
    }

    @Test
    public void testQueryWithPositionalParamsViaCall() {
        testCall(db, "CALL apoc.couchbase.posParamsQuery($host, $bucket, $query, ['Van Gogh'])",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "query", "select * from " + CouchbaseTestUtils.BUCKET_NAME + " where lastName = $1"),
                r -> CouchbaseTestUtils.checkListResult(r));
    }

    @Test
    public void testQueryWithNamedParamsViaCall() {
        testCall(db, "CALL apoc.couchbase.namedParamsQuery($host, $bucket, $query, ['lastName'], ['Van Gogh'])",
                map("host", CouchbaseTestUtils.HOST, "bucket", CouchbaseTestUtils.BUCKET_NAME, "query", "select * from " + CouchbaseTestUtils.BUCKET_NAME + " where lastName = $lastName"),
                r -> CouchbaseTestUtils.checkListResult(r));
    }
}
