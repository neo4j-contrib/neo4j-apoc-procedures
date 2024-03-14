package apoc.vectordb;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.BODY_KEY;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.ml.RestAPIConfig.JSON_PATH_KEY;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static apoc.vectordb.VectorEmbeddingConfig.EMBEDDING_KEY;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * It leverages `apoc.vectordb.custom*` procedures
 * *
 * *
 * Example of Pinecone RestAPI:
 * PINECONE_HOST: `https://INDEX-ID.svc.gcp-starter.pinecone.io`
 * PINECONE_KEY: `API Key`
 * PINECONE_NAMESPACE: `the one to be specified in body: {.. "ns": NAMESPACE}`
 * PINECONE_DIMENSION: vector dimension
 */
public class PineconeTest {
    private static String apiKey;
    private static String host;
    private static String size;
    private static String namespace;
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    
    @BeforeClass
    public static void setUp() throws Exception {
        apiKey = extracted("PINECONE_KEY");
        host = extracted("PINECONE_HOST");
        size = extracted("PINECONE_DIMENSION");
        namespace = extracted("PINECONE_NAMESPACE");
        
        TestUtil.registerProcedure(db, VectorDb.class);
    }

    private static String extracted(String envKey) {
        String size = System.getenv(envKey);
        Assume.assumeNotNull("No %s environment configured".formatted(envKey), size);
        return size;
    }


    @Test
    public void callQueryEndpointViaCustomGetProc() {

        Map<String, Object> conf = getConf();
        conf.put(EMBEDDING_KEY, "values");

        testResult(db, "CALL apoc.vectordb.custom.get($host, $conf)",
                map("host", host + "/query", "conf", conf),
                r -> {
                    r.forEachRemaining(i -> {
                        assertNotNull(i.get("score"));
                        assertNotNull(i.get("metadata"));
                        assertNotNull(i.get("id"));
                        assertNotNull(i.get("vector"));
                    });
                });
    }

    @Test
    public void callQueryEndpointViaCustomProc() {
        testCall(db, "CALL apoc.vectordb.custom($host, $conf)",
                map("host", host + "/query", "conf", getConf()),
                r -> {
                    List<Map> value = (List<Map>) r.get("value");
                    value.forEach(i -> {
                        assertTrue(i.containsKey("score"));
                        assertTrue(i.containsKey("metadata"));
                        assertTrue(i.containsKey("id"));
                    });
                });
    }

    /**
     * TODO: "method" is null as a workaround.
     *  Since with `method: POST` the {@link apoc.util.Util#openUrlConnection(URL, Map)} has a `setChunkedStreamingMode`
     *  that makes the request to respond 200 OK, but returns an empty result 
     */
    private static Map<String, Object> getConf() {
        List<Double> vector = Collections.nCopies(Integer.parseInt(size), 0.1);

        Map<String, Object> body = map(
                "namespace", namespace, 
                "vector", vector, 
                "topK", 3,
                "includeValues", true,
                "includeMetadata", true
        );

        Map<String, Object> header = map("Api-Key", apiKey);

        return map(BODY_KEY, body,
                HEADERS_KEY, header,
                METHOD_KEY, null,
                JSON_PATH_KEY, "matches");
    }
}
