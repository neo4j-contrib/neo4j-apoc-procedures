package apoc.couchbase;

import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
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

import static org.junit.Assert.*;

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

    public static final String USERNAME = "admin";

    public static final String PASSWORD = "secret";

    private static final String QUERY = "select * from %s where lastName = 'Van Gogh'";

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

    public static Bucket getCouchbaseBucket(CouchbaseContainer couchbase) {
        int version = getVersion(couchbase);
        if (version == 4) {
            return couchbase.getCouchbaseCluster().openBucket(BUCKET_NAME, PASSWORD);
        } else {
            return couchbase.getCouchbaseCluster().authenticate(USERNAME, PASSWORD).openBucket(BUCKET_NAME);
        }

    }

    public static int getVersion(CouchbaseContainer couchbase) {
        return couchbase.getCouchbaseCluster().clusterManager(USERNAME, PASSWORD).info(1, TimeUnit.SECONDS).getMinVersion().major();
    }

    public static String getBucketName(CouchbaseContainer couchbase) {
        int version = getVersion(couchbase);
        if (version == 4) {
            return BUCKET_NAME + ":" + PASSWORD;
        } else {
            return BUCKET_NAME;
        }
    }

}
