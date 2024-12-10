package apoc.util;

import org.testcontainers.weaviate.WeaviateContainer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.Util.map;
import static apoc.vectordb.VectorDbTestUtil.EntityType.FALSE;
import static apoc.vectordb.VectorDbTestUtil.assertBerlinResult;
import static apoc.vectordb.VectorDbTestUtil.assertLondonResult;
import static apoc.vectordb.VectorDbTestUtil.getAuthHeader;
import static org.junit.Assert.assertNotNull;

public class WeaviateTestUtil {
    public static final List<String> FIELDS = List.of("city", "foo");
    public static final String ADMIN_KEY = "jane-secret-key";
    public static final String READONLY_KEY = "ian-secret-key";
    public static final String COLLECTION_NAME = "TestCollection";

    public static final WeaviateContainer WEAVIATE_CONTAINER = new WeaviateContainer("semitechnologies/weaviate:1.24.5")
            .withEnv("AUTHENTICATION_APIKEY_ENABLED", "true")
            .withEnv("AUTHENTICATION_APIKEY_ALLOWED_KEYS", ADMIN_KEY + "," + READONLY_KEY)
            .withEnv("AUTHENTICATION_APIKEY_USERS", "jane@doe.com,ian-smith")
            .withEnv("AUTHORIZATION_ADMINLIST_ENABLED", "true")
            .withEnv("AUTHORIZATION_ADMINLIST_USERS", "jane@doe.com,john@doe.com")
            .withEnv("AUTHORIZATION_ADMINLIST_READONLY_USERS", "ian-smith,roberta@doe.com");

    public static final Map<String, String> ADMIN_AUTHORIZATION = getAuthHeader(ADMIN_KEY);
    public static final Map<String, String> READONLY_AUTHORIZATION = getAuthHeader(READONLY_KEY);
    public static final Map<String, Object> ADMIN_HEADER_CONF = map(HEADERS_KEY, ADMIN_AUTHORIZATION);

    public static final String ID_1 = "8ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";
    public static final String ID_2 = "9ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";

    public static String HOST_WEAVIATE;
    public static final int WEAVIATE_PORT = 8080;

    public static final String WEAVIATE_CREATE_COLLECTION_APOC = "CALL apoc.vectordb.weaviate.createCollection($host, 'TestCollection', 'cosine', 4, $conf)";

    public static final String WEAVIATE_DELETE_COLLECTION_APOC = "CALL apoc.vectordb.weaviate.deleteCollection($host, $collectionName, $conf)";

    public static final String WEAVIATE_QUERY_APOC = "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) YIELD score, vector, id, metadata RETURN * ORDER BY id";

    public static final String WEAVIATE_DELETE_VECTOR_APOC = "CALL apoc.vectordb.weaviate.delete($host, 'TestCollection', ['7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308', '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309'], $conf)";

    public static final String WEAVIATE_UPSERT_QUERY = """
                CALL apoc.vectordb.weaviate.upsert($host, 'TestCollection',
                [
                    {id: $id1, vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: "Berlin", foo: "one"}},
                    {id: $id2, vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: "London", foo: "two"}},
                    {id: '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}},
                    {id: '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}}
                ],
                $conf)
                """;

    public static void queryVectorsAssertions(Iterator<Map<String, Object>> r) {
        Map<String, Object> row = r.next();
        assertBerlinResult(row, ID_1, FALSE);
        assertNotNull(row.get("score"));
        assertNotNull(row.get("vector"));

        row = r.next();
        assertLondonResult(row, ID_2, FALSE);
        assertNotNull(row.get("score"));
        assertNotNull(row.get("vector"));
    }
}
