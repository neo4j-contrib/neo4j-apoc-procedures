package apoc.vectordb;

import apoc.util.TestUtil;
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
import static apoc.util.UtilsExtendedTest.checkEnvVar;
import static apoc.vectordb.VectorEmbeddingConfig.VECTOR_KEY;
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
public class PineconeCustomTest {
    private static String apiKey;
    private static String host;
    private static String size;
    private static String namespace;
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    
    @BeforeClass
    public static void setUp() throws Exception {
        apiKey = checkEnvVar("PINECONE_KEY");
        host = checkEnvVar("PINECONE_HOST");
        size = checkEnvVar("PINECONE_DIMENSION");
        namespace = checkEnvVar("PINECONE_NAMESPACE");
        
        TestUtil.registerProcedure(db, VectorDb.class);
    }

    @Test
    public void callQueryEndpointViaCustomGetProc() {

        Map<String, Object> conf = getConf();
        conf.put(VECTOR_KEY, "values");

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
