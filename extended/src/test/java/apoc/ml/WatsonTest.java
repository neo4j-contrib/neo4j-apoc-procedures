package apoc.ml;

import apoc.ml.watson.Watson;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_ML_WATSON_PROJECT_ID;
import static apoc.ExtendedApocConfig.APOC_ML_WATSON_URL;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;


public class WatsonTest {

    private static ClientAndServer mockServer;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static final String accessToken = "mocked";

    @BeforeClass
    public static void startServer() throws Exception {
        TestUtil.registerProcedure(db, Watson.class);
        
        String path = "/generation/text";
        apocConfig().setProperty(APOC_ML_WATSON_URL, "http://localhost:1080" + path);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_ML_WATSON_PROJECT_ID, "fakeProjectId");
        
        File urlFileName = new File(getUrlFileName("watson.json").getFile());
        String body = FileUtils.readFileToString(urlFileName, UTF_8);
        
        mockServer = startClientAndServer(1080);
        mockServer.when(
                        request()
                                .withMethod("POST")
                                .withPath(path)
                                .withHeader("Authorization", "Bearer " + accessToken)
                                .withHeader("Content-Type", "application/json")
                                .withHeader(  "accept", "application/json")
                )
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeaders(
                                        new Header("Cache-Control", "private, max-age=1000"))
                                .withBody(body)
                );
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }
    
    
    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.watson.completion('What color is the sky? Answer in one word: ', $accessToken)",
                Map.of("accessToken", accessToken),
                WatsonTest::commonAssertions);
    }
    
    @Test
    public void chatCompletion() {
        testCall(db, """
                    CALL apoc.ml.watson.chat([
                        {role:"system", content:"Only answer with a single word"},
                        {role:"user", content:"What planet do humans live on?"}
                    ],  $apiKey)""",
                Map.of("apiKey",accessToken),
                WatsonTest::commonAssertions);
    }

    private static void commonAssertions(Map<String, Object> row) {
        var res = (Map<String, Object>) row.get("value");
        assertNotNull(res.get("created_at"));
        assertNotNull(res.get("model_id"));

        List<Map> results = (List<Map>) res.get("results");
        assertEquals(1, results.size());

        Map result = results.get(0);
        String generatedText = (String) result.get("generated_text");
        assertTrue(generatedText.toLowerCase().contains("earth"));
        assertEquals(19L, result.get("input_token_count"));
        assertEquals("max_tokens", result.get("stop_reason"));
    }
}
