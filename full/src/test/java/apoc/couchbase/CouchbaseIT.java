package apoc.couchbase;

import apoc.util.TestUtil;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.couchbase.CouchbaseTestUtils.*;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class CouchbaseIT {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        createCouchbaseContainer();

        Map<String, Object> properties = Map.of(
                BASE_CONFIG_KEY + CouchbaseManager.URI_CONFIG_KEY, "localhost",
                BASE_CONFIG_KEY + CouchbaseManager.USERNAME_CONFIG_KEY, USERNAME,
                BASE_CONFIG_KEY + CouchbaseManager.PASSWORD_CONFIG_KEY, PASSWORD
        );
        properties.forEach((key, value) -> apocConfig().setProperty(key, value));

        TestUtil.registerProcedure(db, Couchbase.class);
    }

    @AfterClass
    public static void tearDown() {
        if (couchbase != null) {
            couchbase.stop();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetViaCall() {
        testCall(db, "CALL apoc.couchbase.get($host, $bucket, 'artist:vincent_van_gogh')",
                map("host", HOST, "bucket", BUCKET_NAME),
                r -> {
                    assertTrue(r.get("content") instanceof Map);
                    Map<String, Object> content = (Map<String, Object>) r.get("content");
                    assertTrue(content.get("notableWorks") instanceof List);
                    List<String> notableWorks = (List<String>) content.get("notableWorks");
                    checkDocumentContent(
                            (String) content.get("firstName"),
                            (String) content.get("secondName"),
                            (String) content.get("lastName"),
                            notableWorks);
                });
    }

    @Test
    public void testExistsViaCallEmptyResult() {
        testCallEmpty(db, "CALL apoc.couchbase.get($host, $bucket, 'notExists')",
                map("host", HOST, "bucket", BUCKET_NAME));
    }

    @Test
    public void testExistsViaCall() {
        testCall(db, "CALL apoc.couchbase.exists($host, $bucket, 'artist:vincent_van_gogh')",
                map("host", HOST, "bucket", BUCKET_NAME),
                r -> assertTrue((boolean) r.get("value")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsertViaCall() {
        testCall(db, "CALL apoc.couchbase.insert($host, $bucket, 'testInsertViaCall', $data)",
                map("host", HOST, "bucket", BUCKET_NAME, "data", VINCENT_VAN_GOGH.toString()),
                r -> {
                    assertTrue(r.get("content") instanceof Map);
                    Map<String, Object> content = (Map<String, Object>) r.get("content");
                    assertTrue(content.get("notableWorks") instanceof List);
                    List<String> notableWorks = (List<String>) content.get("notableWorks");
                    checkDocumentContent(
                            (String) content.get("firstName"),
                            (String) content.get("secondName"),
                            (String) content.get("lastName"),
                            notableWorks);
                    collection.remove("testInsertViaCall");
                    assertFalse(collection.exists("testInsertViaCall").exists());
                });
    }

    @Test
    public void testAppendViaCall() {
        String input = "hello";
        byte[] bytes = input.getBytes();

        final String expectedId = "binaryId";
        collection.insert(expectedId, bytes, InsertOptions.insertOptions().transcoder(RawBinaryTranscoder.INSTANCE));

        testCall(db, "CALL apoc.couchbase.append($host, $bucket, 'binaryId', $data)",
                map("host", HOST, "bucket", BUCKET_NAME, "data", " {from: 'world'}".getBytes()),
                r -> {
                    final String actualContent = new String((byte[]) r.get("content"));
                    final String actualId = (String) r.get("id");
                    assertEquals(expectedId, actualId);
                    assertEquals("hello {from: 'world'}", actualContent);
                    collection.remove(expectedId);
                    assertFalse(collection.exists(expectedId).exists());
                });
    }

    @Test
    public void testPrependViaCall() {
        String input = " world";
        byte[] bytes = input.getBytes();

        final String expectedId = "binaryId";
        collection.insert(expectedId, bytes, InsertOptions.insertOptions().transcoder(RawBinaryTranscoder.INSTANCE));

        testCall(db, "CALL apoc.couchbase.prepend($host, $bucket, 'binaryId', $data)",
                map("host", HOST, "bucket", BUCKET_NAME, "data", "hello".getBytes()),
                r -> {
                    final String actualContent = new String((byte[]) r.get("content"));
                    final String actualId = (String) r.get("id");
                    assertEquals(expectedId, actualId);
                    assertEquals("hello world", actualContent);
                    collection.remove(expectedId);
                    assertFalse(collection.exists(expectedId).exists());
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testInsertWithAlreadyExistingIDViaCall() {
        testCall(db, "CALL apoc.couchbase.insert($host, $bucket, 'artist:vincent_van_gogh', $data)",
                map("host", HOST, "bucket", BUCKET_NAME, "data", VINCENT_VAN_GOGH.toString()),
                r -> {});
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpsertViaCall() {
        testCall(db, "CALL apoc.couchbase.upsert($host, $bucket, 'testUpsertViaCall', $data)",
                map("host", HOST, "bucket", BUCKET_NAME, "data", VINCENT_VAN_GOGH.toString()),
                r -> {
                    assertTrue(r.get("content") instanceof Map);
                    Map<String, Object> content = (Map<String, Object>) r.get("content");
                    assertTrue(content.get("notableWorks") instanceof List);
                    List<String> notableWorks = (List<String>) content.get("notableWorks");
                    checkDocumentContent(
                            (String) content.get("firstName"),
                            (String) content.get("secondName"),
                            (String) content.get("lastName"),
                            notableWorks);
                    collection.remove("testUpsertViaCall");
                    assertFalse(collection.exists("testUpsertViaCall").exists());
                });
    }

    @Test
    public void testRemoveViaCall() {
        collection.insert("testRemove", JsonObject.create());
        testCall(db, "CALL apoc.couchbase.remove($host, $bucket, 'testRemove')",
                map("host", HOST, "bucket", BUCKET_NAME),
                r -> assertFalse(collection.exists("testRemove").exists()));
    }

    @Test
    public void testQueryViaCall() {
        testCall(db, "CALL apoc.couchbase.query($host, $bucket, $query)",
                map("host", HOST, "bucket", BUCKET_NAME, "query", "select * from " + BUCKET_NAME + " where lastName = \"Van Gogh\""),
                r -> checkListResult(r));
    }

    @Test
    public void testQueryViaCallEmptyResult() {
        testCallEmpty(db, "CALL apoc.couchbase.query($host, $bucket, $query)",
                map("host", HOST, "bucket", BUCKET_NAME, "query", "select * from " + BUCKET_NAME + " where lastName = 'notExists'"));
    }

    @Test
    public void testQueryWithPositionalParamsViaCall() {
        testCall(db, "CALL apoc.couchbase.posParamsQuery($host, $bucket, $query, ['Van Gogh'])",
                map("host", HOST, "bucket", BUCKET_NAME, "query", "select * from " + BUCKET_NAME + " where lastName = $1"),
                r -> checkListResult(r));
    }

    @Test
    public void testQueryWithNamedParamsViaCall() {
        testCall(db, "CALL apoc.couchbase.namedParamsQuery($host, $bucket, $query, ['lastName'], ['Van Gogh'])",
                map("host", HOST, "bucket", BUCKET_NAME, "query", "select * from " + BUCKET_NAME + " where lastName = $lastName"),
                r -> checkListResult(r));
    }
}
