package apoc.couchbase;

import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.util.TestUtil;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;

import static apoc.util.TestUtil.isRunningInCI;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assume.assumeNotNull;

public class CouchbaseTestUtils {

    public static final String BUCKET_NAME = "mybucket";

    public static final String USERNAME = "admin";
    public static final String PASSWORD = "secret";

    private static final String QUERY = "select * from %s where lastName = 'Van Gogh'";

    protected static final String COUCHBASE_CONFIG_KEY = "demo";
    protected static final String BASE_APOC_CONFIG = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY;
    protected static final String BASE_CONFIG_KEY = BASE_APOC_CONFIG + COUCHBASE_CONFIG_KEY + ".";

    protected static CouchbaseContainer couchbase;
    protected static Collection collection;
    protected static String HOST = null;

    public static final JsonObject VINCENT_VAN_GOGH = JsonObject.create()
            .put("firstName", "Vincent")
            .put("secondName", "Willem")
            .put("lastName", "Van Gogh")
            .put("notableWorks", JsonArray.from("Starry Night", "Sunflowers", "Bedroom in Arles", "Portrait of Dr Gachet", "Sorrow"));

    public static boolean fillDB(Cluster cluster) {
        Bucket couchbaseBucket = cluster.bucket(BUCKET_NAME);
        Collection collection = couchbaseBucket.defaultCollection();
        collection.insert("artist:vincent_van_gogh", VINCENT_VAN_GOGH);
        QueryResult queryResult = cluster.query(String.format(QUERY, BUCKET_NAME),
                queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
        return queryResult.rowsAsObject().size() == 1;
    }

    public static String getUrl(CouchbaseContainer couchbaseContainer) {
        return String.format("couchbase://%s:%s@%s:%s", USERNAME, PASSWORD, couchbaseContainer.getContainerIpAddress(), couchbaseContainer.getMappedPort(8091));
    }
    
    @SuppressWarnings("unchecked")
    public static void checkListResult(Map<String, Object> r) {
        assertTrue(r.get("queryResult") instanceof List);
        List<Map<String, Object>> listResult = (List<Map<String, Object>>) r.get("queryResult");
        assertNotNull(listResult);
        assertEquals(1, listResult.size());
        assertTrue(listResult.get(0) instanceof Map);
        Map<String, Object> result = (Map<String, Object>) listResult.get(0);
        checkResult(result);
    }

    @SuppressWarnings("unchecked")
    public static void checkResult(Map<String, Object> result) {
        assertTrue(result.get(BUCKET_NAME) instanceof Map);
        Map<String, Object> content = (Map<String, Object>) result.get(BUCKET_NAME);
        assertTrue(content.get("notableWorks") instanceof List);
        List<String> notableWorks = (List<String>) content.get("notableWorks");
        //@eclipse-formatter:off
        checkDocumentContent(
                (String) content.get("firstName"),
                (String) content.get("secondName"),
                (String) content.get("lastName"),
                notableWorks);
        //@eclipse-formatter:on
    }

    public static void checkDocumentContent(String firstName, String secondName, String lastName, List<String> notableWorks) {
        assertEquals("Vincent", firstName);
        assertEquals("Willem", secondName);
        assertEquals("Van Gogh", lastName);
        assertEquals(5, notableWorks.size());
        assertTrue(notableWorks.contains("Starry Night"));
        assertTrue(notableWorks.contains("Sunflowers"));
        assertTrue(notableWorks.contains("Bedroom in Arles"));
        assertTrue(notableWorks.contains("Portrait of Dr Gachet"));
        assertTrue(notableWorks.contains("Sorrow"));
    }

    protected static void checkDocumentMetadata(CouchbaseJsonDocument jsonDocumentCreatedForThisTest, String id, long expiry, long cas, Map<String, Object> mutationToken) {
        assertEquals(jsonDocumentCreatedForThisTest.id, id);
        assertEquals(jsonDocumentCreatedForThisTest.expiry, expiry);
        assertEquals(jsonDocumentCreatedForThisTest.cas, cas);
        assertEquals(jsonDocumentCreatedForThisTest.mutationToken, mutationToken);
    }

    protected static void createCouchbaseContainer() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            couchbase = new CouchbaseContainer()
                    .withCredentials(USERNAME, PASSWORD)
                    .withBucket(new BucketDefinition(BUCKET_NAME));
            couchbase.start();
        }, Exception.class);
        assumeNotNull(couchbase);
        assumeTrue("couchbase must be running", couchbase.isRunning());

        ClusterEnvironment environment = ClusterEnvironment.create();

        Set<SeedNode> seedNodes = Set.of(SeedNode.create(couchbase.getHost(),
                Optional.of(couchbase.getBootstrapCarrierDirectPort()),
                Optional.of(couchbase.getBootstrapHttpDirectPort())));

        Cluster cluster = Cluster.connect(seedNodes, clusterOptions(USERNAME, PASSWORD).environment(environment));

        boolean isFilled = fillDB(cluster);
        assumeTrue("should fill Couchbase with data", isFilled);
        HOST = getUrl(couchbase);
        Bucket bucket = cluster.bucket(BUCKET_NAME);
        collection = bucket.defaultCollection();
    }

}
