package apoc.ml;

import apoc.ApocConfig;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ExtendedApocConfig.APOC_ML_WATSON_URL;
import static apoc.ExtendedApocConfig.APOC_ML_WATSON_PROJECT_ID;
import static apoc.ml.MLTestUtil.assertNullInputFails;
import static apoc.util.TestUtil.testCall;
import static java.util.Collections.emptyMap;
import static org.junit.Assume.assumeNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Generate accessToken via:
 * ```
 * curl -X POST 'https://iam.cloud.ibm.com/identity/token' -H 'Content-Type: application/x-www-form-urlencoded' -d 'grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=<API_KEY>'
 * ```
 * 
 * The `WATSON_ACCESS_TOKEN` (to populate the 2nd parameter) and `WATSON_PROJECT_ID` (to populate the `project_id` request key) env vars are mandatory.
 * The `WATSON_ENDPOINT_URL` env var (to define the endpoint url) is optional (with default: `https://eu-de.ml.cloud.ibm.com/ml/v1-beta/generation/text?version=2023-05-29`)
 */
public class WatsonIT {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static String accessToken;

    @BeforeClass
    public static void setUp() throws Exception {
        String keyIdEnv = "WATSON_ACCESS_TOKEN";
        String projectIdEnv = "WATSON_PROJECT_ID";

        accessToken = System.getenv(keyIdEnv);
        String projectId = System.getenv(projectIdEnv);
        
        assumeNotNull(keyIdEnv + "environment not configured", accessToken);
        assumeNotNull(projectIdEnv + "environment not configured", projectId);

        ApocConfig.apocConfig().setProperty(APOC_ML_WATSON_PROJECT_ID, projectId);


        String endpointEnv = "WATSON_ENDPOINT_URL";
        String endpoint = System.getenv(endpointEnv);
        if (endpoint != null) {
            ApocConfig.apocConfig().setProperty(APOC_ML_WATSON_URL, projectId);
        }

        TestUtil.registerProcedure(db, Watson.class);
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.watson.completion('What color is the sky? Answer in one word: ', $accessToken)",
                Map.of("accessToken", accessToken),
                (row) -> {
                    commonAssertions(row, "blue", 12L, "eos_token");
                });
    }

    @Test
    public void completionWithParameters() {
        testCall(db, "CALL apoc.ml.watson.completion('What color is the sky? Answer in one word: ', $accessToken, {parameters: {max_new_tokens: 1}})",
                Map.of("accessToken", accessToken),
                (row) -> {
                    commonAssertions(row, "\n", 12L, "max_tokens");
                });
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
                    CALL apoc.ml.watson.chat([
                        {role:"system", content:"Only answer with a single word"},
                        {role:"user", content:"What planet do humans live on?"}
                    ],  $apiKey)""",
                Map.of("apiKey",accessToken), 
                (row) -> {
                    commonAssertions(row, "earth", 19L, "max_tokens");
                });
    }

    @Test
    public void chatCompletionWithParameters() {
        testCall(db, """
                    CALL apoc.ml.watson.chat([
                        {role:"system", content:"Only answer with a single word"},
                        {role:"user", content:"What planet do humans live on?"}
                    ],  $apiKey, {parameters: {max_new_tokens: 1}})""",
                Map.of("apiKey",accessToken), 
                (row) -> commonAssertions(row, "\n", 19L, "max_tokens"));
    }
    
    @Test
    public void wrongEndpoint() {
        try {
            testCall(db, """
                            CALL apoc.ml.watson.chat([
                                {role:"system", content:"Only answer with a single word"},
                                {role:"user", content:"What planet do humans live on?"}
                            ],  $apiKey, {endpoint: 'https://wrong/endpoint'})""",
                    Map.of("apiKey", accessToken),
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
