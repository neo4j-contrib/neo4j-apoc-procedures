package apoc.couchbase;

import apoc.util.TestUtil;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.List;
import java.util.Map;

import static apoc.couchbase.CouchbaseTestUtils.BUCKET_NAME;
import static apoc.couchbase.CouchbaseTestUtils.COMPUTATION_POOL_SIZE_CONFIG_KEY;
import static apoc.couchbase.CouchbaseTestUtils.COMPUTATION_POOL_SIZE_CONFIG_VALUE;
import static apoc.couchbase.CouchbaseTestUtils.CONNECTION_TIMEOUT_CONFIG_KEY;
import static apoc.couchbase.CouchbaseTestUtils.CONNECTION_TIMEOUT_CONFIG_VALUE;
import static apoc.couchbase.CouchbaseTestUtils.IO_POOL_SIZE_CONFIG_KEY;
import static apoc.couchbase.CouchbaseTestUtils.IO_POOL_SIZE_CONFIG_VALUE;
import static apoc.couchbase.CouchbaseTestUtils.KV_TIMEOUT_CONFIG_KEY;
import static apoc.couchbase.CouchbaseTestUtils.KV_TIMEOUT_CONFIG_VALUE;
import static apoc.couchbase.CouchbaseTestUtils.PASSWORD;
import static apoc.couchbase.CouchbaseTestUtils.SOCKET_CONNECT_TIMEOUT_CONFIG_KEY;
import static apoc.couchbase.CouchbaseTestUtils.SOCKET_CONNECT_TIMEOUT_CONFIG_VALUE;
import static apoc.couchbase.CouchbaseTestUtils.USERNAME;
import static apoc.couchbase.CouchbaseTestUtils.VINCENT_VAN_GOGH;
import static apoc.couchbase.CouchbaseTestUtils.checkDocumentContent;
import static apoc.couchbase.CouchbaseTestUtils.checkListResult;
import static apoc.couchbase.CouchbaseTestUtils.createCluster;
import static apoc.couchbase.CouchbaseTestUtils.fillDB;
import static apoc.couchbase.CouchbaseTestUtils.getCouchbaseBucket;
import static apoc.couchbase.CouchbaseTestUtils.getUrl;
import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

@Ignore("By updating the TestContainer Version it breaks, as is a very old version we can skip it")
public class CouchbaseV4IT {

    private static String HOST = null;

    private static GraphDatabaseService graphDB;

    private static Bucket couchbaseBucket;

    public static CouchbaseContainer couchbase;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.ignoreException(() -> {
            couchbase = new CouchbaseContainer("couchbase/server:4.6.5")
                    .withCredentials(USERNAME, PASSWORD);
            couchbase.start();
        }, Exception.class);
        assumeNotNull(couchbase);
        assumeTrue("couchbase must be running", couchbase.isRunning());
        final CouchbaseCluster cluster = createCluster(couchbase);
        cluster.clusterManager(USERNAME, PASSWORD).insertBucket(DefaultBucketSettings.builder()
                .password(PASSWORD)
                .name(BUCKET_NAME)
                .port(couchbase.getMappedPort(8091))
                .quota(100)
                .type(BucketType.COUCHBASE)
                .build());
        boolean isFilled = fillDB(cluster);
        assumeTrue("should fill Couchbase with data", isFilled);
        HOST = getUrl(couchbase);
        couchbaseBucket = getCouchbaseBucket(cluster);
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

    private static boolean isBucketReady(CouchbaseCluster cluster) {
        return cluster.openBucket(BUCKET_NAME).core()
                .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .toBlocking().single().config()
                .bucketConfig(BUCKET_NAME)
                .serviceEnabled(ServiceType.QUERY);
    }

    @AfterClass
    public static void tearDown() {
        if (couchbase != null && couchbase.isRunning()) {
            couchbase.stop();
            if (graphDB != null) {
                graphDB.shutdown();
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.get({host}, {bucket}, 'artist:vincent_van_gogh')",
                map("host", HOST, "bucket", BUCKET_NAME + ":" + PASSWORD),
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
                map("host", HOST, "bucket", BUCKET_NAME + ":" + PASSWORD),
                r -> assertTrue((boolean) r.get("value")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsertViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.insert({host}, {bucket}, 'testInsertViaCall', {data})",
                map("host", HOST, "bucket", BUCKET_NAME + ":" + PASSWORD, "data", VINCENT_VAN_GOGH.toString()),
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
                map("host", HOST, "bucket", BUCKET_NAME + ":" + PASSWORD, "data", VINCENT_VAN_GOGH.toString()),
                r -> {});
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpsertViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.upsert({host}, {bucket}, 'testUpsertViaCall', {data})",
                map("host", HOST, "bucket", BUCKET_NAME + ":" + PASSWORD, "data", VINCENT_VAN_GOGH.toString()),
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
                map("host", HOST, "bucket", BUCKET_NAME + ":" + PASSWORD),
                r -> assertFalse(couchbaseBucket.exists("testRemove")));
    }

    @Test
    public void testQueryViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.query({host}, {bucket}, {query})",
                map("host", HOST, "bucket", BUCKET_NAME + ":" + PASSWORD, "query", "select * from " + BUCKET_NAME + " where lastName = \"Van Gogh\""),
                r -> checkListResult(r));
    }

    @Test
    public void testQueryWithPositionalParamsViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.posParamsQuery({host}, {bucket}, {query}, ['Van Gogh'])",
                map("host", HOST, "bucket", BUCKET_NAME + ":" + PASSWORD, "query", "select * from " + BUCKET_NAME + " where lastName = $1"),
                r -> checkListResult(r));
    }

    @Test
    public void testQueryWithNamedParamsViaCall() {
        testCall(graphDB, "CALL apoc.couchbase.namedParamsQuery({host}, {bucket}, {query}, ['lastName'], ['Van Gogh'])",
                map("host", HOST, "bucket", BUCKET_NAME + ":" + PASSWORD, "query", "select * from " + BUCKET_NAME + " where lastName = $lastName"),
                r -> checkListResult(r));
    }
}