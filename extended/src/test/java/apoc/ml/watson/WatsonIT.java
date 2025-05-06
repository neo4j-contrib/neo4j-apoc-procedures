package apoc.ml.watson;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_ML_WATSON_PROJECT_ID;
import static apoc.ml.MLTestUtil.assertNullInputFails;
import static apoc.ml.MLUtil.*;
import static apoc.ml.watson.Watson.DEFAULT_REGION;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyMap;
import static org.junit.Assume.assumeNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Generate accessToken via:
 * ```
 * curl -X POST 'https://iam.cloud.ibm.com/identity/token' -H 'Content-Type: application/x-www-form-urlencoded' -d 'grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=<API_KEY>'
 * ```
 * 
 * The `WATSON_ACCESS_TOKEN` (to populate the 2nd parameter) and `WATSON_PROJECT_ID` (to populate the `project_id` request key) env vars are mandatory.
 * The `WATSON_MODEL_ENDPOINT_URL` env var (to define the endpoint url) is optional (with default: `https://eu-de.ml.cloud.ibm.com/ml/v1-beta/generation/text?version=2023-05-29`)
 * The `WATSON_ENDPOINT_REGION` (to define the endpoint url), is optional (default: `eu-de`)
 *      it will call the endpoint: `https://{<WATSON_ENDPOINT_REGION>}.ml.cloud.ibm.com/ml/v1/{METHOD}?version=2023-05-29`),
 *      where METHOD is `text/embeddings` for the apoc.ml.watson.embedding, otherwise is `text/generation`
 * 
 */
public class WatsonIT {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static String accessToken;
    private static String endpointRegion;

    @BeforeClass
    public static void setUp() throws Exception {
        String keyIdEnv = "WATSON_ACCESS_TOKEN";
        String projectIdEnv = "WATSON_PROJECT_ID";

        accessToken = System.getenv(keyIdEnv);
        String projectId = System.getenv(projectIdEnv);
        
        assumeNotNull(keyIdEnv + "environment not configured", accessToken);
        assumeNotNull(projectIdEnv + "environment not configured", projectId);

        apocConfig().setProperty(APOC_ML_WATSON_PROJECT_ID, projectId);

        String regionEnv = System.getenv("WATSON_ENDPOINT_REGION");
        
        endpointRegion = regionEnv == null
                ? DEFAULT_REGION
                : regionEnv;

        TestUtil.registerProcedure(db, Watson.class);
    }
    
    @Test
    public void embedding() {
        testResult(db, "CALL apoc.ml.watson.embedding(['Some Text', 'Another Text'], $accessToken, $conf)",
                Map.of("accessToken", accessToken,
                        "conf", Map.of(REGION_CONF_KEY, endpointRegion)
                ),
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(384, ((List) row.get("embedding")).size());
                    assertEquals("Some Text", row.get("text"));
                    row = r.next();
                    
                    assertEquals(384, ((List) row.get("embedding")).size());
                    assertEquals("Another Text", row.get("text"));
                    assertFalse(r.hasNext());
                    
        });
    }
    
    @Test
    public void embeddingWithWrongDate() {
        try {
            testCall(db, "CALL apoc.ml.watson.embedding(['Some Text', 'Another Text'], $accessToken, $conf)",
                    Map.of("accessToken", accessToken,
                            "conf", Map.of(REGION_CONF_KEY, endpointRegion, API_VERSION_CONF_KEY, "2025-33-33 ")
                    ),
                    (row) -> fail());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Server returned HTTP response code: 400"));
        }
    }
    
    @Test
    public void embeddingWithNonDefaultModel() {
        testResult(db, "CALL apoc.ml.watson.embedding(['Some Text', 'Another Text'], $accessToken, $conf)",
                Map.of("accessToken", accessToken, 
                        "conf", Map.of(MODEL_CONF_KEY, "ibm/slate-125m-english-rtrvr", REGION_CONF_KEY, endpointRegion)
                ),
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(384, ((List) row.get("embedding")).size());
                    assertEquals("Some Text", row.get("text"));
                    
                    row = r.next();
                    assertEquals(384, ((List) row.get("embedding")).size());
                    assertEquals("Another Text", row.get("text"));
                    
                    assertFalse(r.hasNext());
                    
        });
    }


    /**
     * Unlike other APIs (such as OpenAI),
     * null can be passed and normally returns an embedding without getting an error 400
     */
    @Test
    public void embeddingWithNulls() {
        testResult(db, "CALL apoc.ml.watson.embedding([null, 'Some Text', null, 'Another Text'], $accessToken, $conf)",
                Map.of("accessToken", accessToken,
                        "conf", Map.of(REGION_CONF_KEY, endpointRegion)
                ),
                (r) -> {

                    Map<String, Object> row = r.next();
                    assertNullEmbedding(row);
                    
                    row = r.next();
                    assertEquals(384, ((List) row.get("embedding")).size());
                    assertEquals("Some Text", row.get("text"));
                    
                    row = r.next();
                    assertNullEmbedding(row);
                    
                    
                    row = r.next();
                    assertEquals(384, ((List) row.get("embedding")).size());
                    assertEquals("Another Text", row.get("text"));
                    
                    assertFalse(r.hasNext());
        });
    }

    private static void assertNullEmbedding(Map<String, Object> row) {
        List embedding = (List) row.get("embedding");
        assertEquals(384, embedding.size());
        
        // check just the first 3 list items for the sake of simplicity 
        assertEquals(0.0644868, embedding.get(0));
        assertEquals(0.012636489, embedding.get(1));
        assertEquals(0.065276936, embedding.get(2));

        assertNull(row.get("text"));
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.watson.completion('What color is the sky? Answer in one word: ', $accessToken, $conf)",
                Map.of("accessToken", accessToken,
                        "conf", Map.of(REGION_CONF_KEY, endpointRegion)
                ),
                (row) -> {
                    commonAssertions(row, "blue", 12L, "max_tokens");
                });
    }

    @Test
    public void completionWithParameters() {
        testCall(db, "CALL apoc.ml.watson.completion('What color is the sky? Answer in one word: ', $accessToken, $conf)",
                Map.of("accessToken", accessToken,
                        "conf", Map.of(REGION_CONF_KEY, endpointRegion, 
                                "parameters", Map.of("max_new_tokens", 1000) 
                        )
                ),
                (row) -> {
                    commonAssertions(row, "\n", 12L, "eos_token");
                });
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
                    CALL apoc.ml.watson.chat([
                        {role:"system", content:"Only answer with a single word"},
                        {role:"user", content:"What planet do humans live on?"}
                    ],  $apiKey, $conf)""",
                Map.of("apiKey", accessToken, 
                        "conf", Map.of(REGION_CONF_KEY, endpointRegion)
                ), 
                (row) -> {
                    commonAssertions(row, "earth", 19L, "eos_token");
                });
    }

    @Test
    public void chatCompletionWithParameters() {
        testCall(db, """
                    CALL apoc.ml.watson.chat([
                        {role:"system", content:"Only answer with a single word"},
                        {role:"user", content:"What planet do humans live on?"}
                    ],  $apiKey, $conf)""",
                Map.of("apiKey", accessToken,
                        "conf", Map.of(REGION_CONF_KEY, endpointRegion,
                                "parameters", Map.of("max_new_tokens", 1000) 
                        )
                ), 
                (row) -> commonAssertions(row, "\n", 19L, "eos_token"));
    }
    
    @Test
    public void wrongEndpoint() {
        try {
            testCall(db, """
                            CALL apoc.ml.watson.chat([
                                {role:"system", content:"Only answer with a single word"},
                                {role:"user", content:"What planet do humans live on?"}
                            ],  $apiKey, $conf)""",
                    Map.of("apiKey", accessToken,
                            "conf", Map.of(REGION_CONF_KEY, endpointRegion, ENDPOINT_CONF_KEY, "https://wrong/endpoint")
                    ),
                    (row) -> fail());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("nodename nor servname provided, or not known"));
        }
    }

    private static void commonAssertions(Map<String, Object> row, String text, long inputTokenCount, String stopReason) {
        var res = (Map<String, Object>) row.get("value");
        assertNotNull(res.get("created_at"));
        assertNotNull(res.get("model_id"));

        List<Map> results = (List<Map>) res.get("results");
        assertEquals(1, results.size());

        Map result = results.get(0);
        String generatedText = (String) result.get("generated_text");
        assertTrue(generatedText.toLowerCase().contains(text));
        assertEquals(inputTokenCount, result.get("input_token_count"));
        assertEquals(stopReason, result.get("stop_reason"));
    }

    @Test
    public void completionNull() {
        assertNullInputFails(db, "CALL apoc.ml.watson.completion(null, $apiKey, $conf)",
                Map.of("apiKey", accessToken, "conf", emptyMap())
        );
    }

    @Test
    public void chatCompletionNull() {
        assertNullInputFails(db, "CALL apoc.ml.watson.chat(null, $apiKey, $conf)",
                Map.of("apiKey", accessToken, "conf", emptyMap())
        );
    }
}
