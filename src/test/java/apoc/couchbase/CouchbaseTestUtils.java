package apoc.couchbase;

import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CouchbaseTestUtils {

    public static final String CONNECTION_TIMEOUT_CONFIG_KEY = "connectTimeout";
    public static final String CONNECTION_TIMEOUT_CONFIG_VALUE = "60000";

    public static final String SOCKET_CONNECT_TIMEOUT_CONFIG_KEY = "socketConnectTimeout";
    public static final String SOCKET_CONNECT_TIMEOUT_CONFIG_VALUE = "10000";

    public static final String KV_TIMEOUT_CONFIG_KEY = "kvTimeout";
    public static final String KV_TIMEOUT_CONFIG_VALUE = "10000";

    public static final String IO_POOL_SIZE_CONFIG_KEY = "ioPoolSize";
    public static final String IO_POOL_SIZE_CONFIG_VALUE = "5";

    public static final String COMPUTATION_POOL_SIZE_CONFIG_KEY = "computationPoolSize";
    public static final String COMPUTATION_POOL_SIZE_CONFIG_VALUE = "5";

    public static final String BUCKET_NAME = "mybucket";

    public static final String USERNAME = "Administrator";

    public static final String PASSWORD = "password";

    private static final String QUERY = "select * from %s where lastName = 'Van Gogh'";

    private static final int MGMT_PORT = 8091;

    private static final int MGMT_SSL_PORT = 18091;

    private static final int VIEW_PORT = 8092;

    private static final int VIEW_SSL_PORT = 18092;

    private static final int QUERY_PORT = 8093;

    private static final int QUERY_SSL_PORT = 18093;

    private static final int SEARCH_PORT = 8094;

    private static final int SEARCH_SSL_PORT = 18094;

    private static final int KV_PORT = 11210;

    private static final int KV_SSL_PORT = 11207;

    public static final JsonObject VINCENT_VAN_GOGH = JsonObject.create()
            .put("firstName", "Vincent")
            .put("secondName", "Willem")
            .put("lastName", "Van Gogh")
            .put("notableWorks", JsonArray.from("Starry Night", "Sunflowers", "Bedroom in Arles", "Portrait of Dr Gachet", "Sorrow"));

    public static boolean fillDB(CouchbaseCluster cluster) {
        Bucket couchbaseBucket = cluster.openBucket(BUCKET_NAME);
        couchbaseBucket.insert(JsonDocument.create("artist:vincent_van_gogh", VINCENT_VAN_GOGH));
        N1qlQueryResult queryResult = couchbaseBucket.query(N1qlQuery.simple(String.format(QUERY, BUCKET_NAME), N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)));
        couchbaseBucket.close();
        return queryResult.info().resultCount() == 1;
    }

    private static DefaultCouchbaseEnvironment createCouchbaseEnvironment(CouchbaseContainer container) {
        return DefaultCouchbaseEnvironment.builder()
                .kvTimeout(10000)
                .bootstrapCarrierDirectPort(container.getMappedPort(KV_PORT))
                .bootstrapCarrierSslPort(container.getMappedPort(KV_SSL_PORT))
                .bootstrapHttpDirectPort(container.getMappedPort(MGMT_PORT))
                .bootstrapHttpSslPort(container.getMappedPort(MGMT_SSL_PORT))
                .build();
    }

    public static CouchbaseCluster createCluster(CouchbaseContainer container) {
        return CouchbaseCluster.create(createCouchbaseEnvironment(container), container.getConnectionString())
                .authenticate(USERNAME, PASSWORD);
    }


    public static String getUrl(CouchbaseContainer couchbaseContainer) {
        return String.format("couchbase://%s:%s@%s:%s", USERNAME, PASSWORD, couchbaseContainer.getContainerIpAddress(), couchbaseContainer.getMappedPort(MGMT_PORT));
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

    public static void checkQueryResult(Stream<CouchbaseQueryResult> stream) {
        Iterator<CouchbaseQueryResult> iterator = stream.iterator();
        assertTrue(iterator.hasNext());
        CouchbaseQueryResult queryResult = iterator.next();
        assertNotNull(queryResult);
        assertEquals(1, queryResult.queryResult.size());
        assertTrue(queryResult.queryResult.get(0) instanceof Map);
        Map<String, Object> result = (Map<String, Object>) queryResult.queryResult.get(0);
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

    public static Bucket getCouchbaseBucket(CouchbaseCluster couchbaseCluster) {
        int version = getVersion(couchbaseCluster);
        if (version == 4) {
            return couchbaseCluster.openBucket(BUCKET_NAME, PASSWORD);
        } else {
            return couchbaseCluster.authenticate(USERNAME, PASSWORD).openBucket(BUCKET_NAME);
        }

    }

    public static int getVersion(CouchbaseCluster couchbaseCluster) {
        return couchbaseCluster.clusterManager(USERNAME, PASSWORD).info(1, TimeUnit.SECONDS).getMinVersion().major();
    }

    public static String getBucketName(CouchbaseCluster couchbaseCluster) {
        int version = getVersion(couchbaseCluster);
        if (version == 4) {
            return BUCKET_NAME + ":" + PASSWORD;
        } else {
            return BUCKET_NAME;
        }
    }

}
