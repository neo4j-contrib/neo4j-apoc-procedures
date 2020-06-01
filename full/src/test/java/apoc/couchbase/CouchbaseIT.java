package apoc.couchbase;

import apoc.util.TestUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.List;
import java.util.Map;

import static apoc.ApocSettings.dynamic;
import static apoc.couchbase.CouchbaseTestUtils.*;
import static apoc.util.TestUtil.isTravis;
import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.*;
import static org.neo4j.configuration.SettingValueParsers.STRING;

public class CouchbaseIT  {

    private static String HOST = null;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
//            .withSetting(dynamic(baseConfigKey + CouchbaseManager.URI_CONFIG_KEY, STRING), "localhost")
//            .withSetting(dynamic(baseConfigKey + CouchbaseManager.USERNAME_CONFIG_KEY, STRING), USERNAME)
//            .withSetting(dynamic(baseConfigKey + CouchbaseManager.PASSWORD_CONFIG_KEY, STRING),  PASSWORD)

            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + CONNECTION_TIMEOUT_CONFIG_KEY, STRING),
                    CONNECTION_TIMEOUT_CONFIG_VALUE)
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + SOCKET_CONNECT_TIMEOUT_CONFIG_KEY, STRING),
                    SOCKET_CONNECT_TIMEOUT_CONFIG_VALUE)
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + KV_TIMEOUT_CONFIG_KEY, STRING),
                    KV_TIMEOUT_CONFIG_VALUE)
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + IO_POOL_SIZE_CONFIG_KEY, STRING),
                    IO_POOL_SIZE_CONFIG_VALUE)
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COMPUTATION_POOL_SIZE_CONFIG_KEY, STRING),
                    COMPUTATION_POOL_SIZE_CONFIG_VALUE);

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
                    couchbaseBucket.remove("testInsertViaCall");
                    assertFalse(couchbaseBucket.exists("testInsertViaCall"));
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
                    couchbaseBucket.remove("testUpsertViaCall");
                    assertFalse(couchbaseBucket.exists("testUpsertViaCall"));
                });
    }

    @Test
    public void testRemoveViaCall() {
        couchbaseBucket.insert(JsonDocument.create("testRemove", JsonObject.empty()));
        testCall(db, "CALL apoc.couchbase.remove($host, $bucket, 'testRemove')",
                map("host", HOST, "bucket", BUCKET_NAME),
                r -> assertFalse(couchbaseBucket.exists("testRemove")));
    }

    @Test
    public void testQueryViaCall() {
        testCall(db, "CALL apoc.couchbase.query($host, $bucket, $query)",
                map("host", HOST, "bucket", BUCKET_NAME, "query", "select * from " + BUCKET_NAME + " where lastName = \"Van Gogh\""),
                r -> checkListResult(r));
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
