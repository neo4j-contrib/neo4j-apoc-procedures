package apoc.couchbase;

import apoc.util.TestUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.List;
import java.util.Map;

import static apoc.couchbase.CouchbaseTestUtils.*;
import static apoc.util.TestUtil.isTravis;
import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.*;

public class CouchbaseIT  {

    private static String HOST = null;

    private static GraphDatabaseService graphDB;

    private static Bucket couchbaseBucket;

    public static CouchbaseContainer couchbase;

    @BeforeClass
    public static void setUp() throws Exception {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            couchbase = new CouchbaseContainer()
                    .withClusterAdmin(USERNAME, PASSWORD)
                    .withNewBucket(DefaultBucketSettings.builder()
                            .password(PASSWORD)
                            .name(BUCKET_NAME)
                            .quota(100)
                            .type(BucketType.COUCHBASE)
                            .build());
            couchbase.start();
        }, Exception.class);
        assumeNotNull(couchbase);
        assumeTrue("couchbase must be running", couchbase.isRunning());
        boolean isFilled = fillDB(couchbase.getCouchbaseCluster());
        assumeTrue("should fill Couchbase with data", isFilled);
        HOST = getUrl(couchbase);
        couchbaseBucket = getCouchbaseBucket(couchbase);
        graphDB = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + CONNECTION_TIMEOUT_CONFIG_KEY,
                        CONNECTION_TIMEOUT_CONFIG_VALUE)
                .setConfig("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + SOCKET_CONNECT_TIMEOUT_CONFIG_KEY,
                        SOCKET_CONNECT_TIMEOUT_CONFIG_VALUE)
                .setConfig("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + KV_TIMEOUT_CONFIG_KEY,
                        KV_TIMEOUT_CONFIG_VALUE)
                .setConfig("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + IO_POOL_SIZE_CONFIG_KEY,
                        IO_POOL_SIZE_CONFIG_VALUE)
                .setConfig("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COMPUTATION_POOL_SIZE_CONFIG_KEY,
                        COMPUTATION_POOL_SIZE_CONFIG_VALUE)
                .newGraphDatabase();
        TestUtil.registerProcedure(graphDB, Couchbase.class);
    }

    @AfterClass
    public static void tearDown() {
        if (couchbase != null) {
            couchbase.stop();
            graphDB.shutdown();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.get({host}, {bucket}, 'artist:vincent_van_gogh')",
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
    public void testExistsViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.exists({host}, {bucket}, 'artist:vincent_van_gogh')",
                map("host", HOST, "bucket", BUCKET_NAME),
                r -> assertTrue((boolean) r.get("value")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsertViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.insert({host}, {bucket}, 'testInsertViaCall', {data})",
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
                    couchbaseBucket.remove("testInsertViaCall");
                    assertFalse(couchbaseBucket.exists("testInsertViaCall"));
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testInsertWithAlreadyExistingIDViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.insert({host}, {bucket}, 'artist:vincent_van_gogh', {data})",
                map("host", HOST, "bucket", BUCKET_NAME, "data", VINCENT_VAN_GOGH.toString()),
                r -> {});
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpsertViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.upsert({host}, {bucket}, 'testUpsertViaCall', {data})",
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
                    couchbaseBucket.remove("testUpsertViaCall");
                    assertFalse(couchbaseBucket.exists("testUpsertViaCall"));
                });
    }

    @Test
    public void testRemoveViaCall() {
        couchbaseBucket.insert(JsonDocument.create("testRemove", JsonObject.empty()));
        testCall(graphDB, "CALL apoc.couchbase.remove({host}, {bucket}, 'testRemove')",
                map("host", HOST, "bucket", BUCKET_NAME),
                r -> assertFalse(couchbaseBucket.exists("testRemove")));
    }

    @Test
    public void testQueryViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.query({host}, {bucket}, {query})",
                map("host", HOST, "bucket", BUCKET_NAME, "query", "select * from " + BUCKET_NAME + " where lastName = \"Van Gogh\""),
                r -> checkListResult(r));
    }

    @Test
    public void testQueryWithPositionalParamsViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.posParamsQuery({host}, {bucket}, {query}, ['Van Gogh'])",
                map("host", HOST, "bucket", BUCKET_NAME, "query", "select * from " + BUCKET_NAME + " where lastName = $1"),
                r -> checkListResult(r));
    }

    @Test
    public void testQueryWithNamedParamsViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.namedParamsQuery({host}, {bucket}, {query}, ['lastName'], ['Van Gogh'])",
                map("host", HOST, "bucket", BUCKET_NAME, "query", "select * from " + BUCKET_NAME + " where lastName = $lastName"),
                r -> checkListResult(r));
    }
}