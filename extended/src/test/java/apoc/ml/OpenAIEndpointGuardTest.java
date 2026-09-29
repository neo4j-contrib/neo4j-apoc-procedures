package apoc.ml;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import apoc.util.RecordingHttpServer;
import apoc.util.RecordingHttpServer.RecordedRequest;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_URL;
import static apoc.ExtendedApocConfig.APOC_OPENAI_KEY;
import static apoc.ml.OpenAI.ERROR_UNTRUSTED_ENDPOINT;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;

/**
 * Mock tests, with a localhost endpoint URL.
 * `localhost` and `127.0.0.1` reach the same mock server, but are different origins:
 * the former plays the trusted endpoint, the latter a caller-chosen one.
 */
public class OpenAIEndpointGuardTest {
    private static final String SERVER_KEY = "server-secret-key";
    private static final String CALLER_KEY = "caller-key";
    private static final String EMBEDDING_QUERY = "CALL apoc.ml.openai.embedding(['Some Text'], $apiKey, $conf)";
    private static RecordingHttpServer server;
    private static String trustedUrl;
    private static String callerUrl;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void startServer() throws Exception {
        TestUtil.registerProcedure(db, OpenAI.class);
        server = new RecordingHttpServer("{\"data\": [{\"index\": 0, \"embedding\": [0.1, 0.2]}]}");
        trustedUrl = "http://localhost:" + server.getPort();
        callerUrl = "http://127.0.0.1:" + server.getPort();
    }

    @AfterClass
    public static void stopServer() {
        server.close();
    }

    @Before
    public void before() {
        server.clear();
        apocConfig().setProperty(APOC_OPENAI_KEY, SERVER_KEY);
    }

    @After
    public void after() {
        apocConfig().getConfig().clearProperty(APOC_OPENAI_KEY);
        apocConfig().getConfig().clearProperty(APOC_ML_OPENAI_URL);
    }

    @Test
    public void serverKeyIsNotSentToCallerEndpoint() {
        assertServerKeyNotSent(map("endpoint", callerUrl));
    }

    @Test
    public void serverKeyIsNotSentToCallerEndpointWithAnthropicApiType() {
        assertServerKeyNotSent(map("endpoint", callerUrl, "apiType", "ANTHROPIC"));
    }

    @Test
    public void serverKeyIsNotSentToCallerEndpointWithAzureApiType() {
        assertServerKeyNotSent(map("endpoint", callerUrl, "apiType", "AZURE"));
    }

    @Test
    public void serverKeyIsNotSentToCallerPath() {
        // HuggingFace has no default endpoint, so the `path` alone would become the full URL
        assertServerKeyNotSent(map("path", callerUrl, "apiType", "HUGGINGFACE"));
    }

    @Test
    public void serverKeyIsNotSentToCallerEndpointEvenIfServerEndpointIsConfigured() {
        apocConfig().setProperty(APOC_ML_OPENAI_URL, trustedUrl);
        assertServerKeyNotSent(map("endpoint", callerUrl));
    }

    @Test
    public void serverKeyIsSentToConfiguredEndpoint() {
        apocConfig().setProperty(APOC_ML_OPENAI_URL, trustedUrl);

        assertEmbeddingSentWith(null, map(), "Bearer " + SERVER_KEY);
    }

    @Test
    public void serverKeyIsSentToExplicitEndpointWithSameOriginAsConfiguredOne() {
        apocConfig().setProperty(APOC_ML_OPENAI_URL, trustedUrl);

        assertEmbeddingSentWith(null, map("endpoint", trustedUrl + "/v2/"), "Bearer " + SERVER_KEY);
    }

    @Test
    public void callerKeyViaConfigIsSentToCallerEndpoint() {
        assertEmbeddingSentWith(null, map("endpoint", callerUrl, "apiKey", CALLER_KEY), "Bearer " + CALLER_KEY);
    }

    @Test
    public void callerKeyViaParameterIsSentToCallerEndpointInsteadOfServerKey() {
        assertEmbeddingSentWith(CALLER_KEY, map("endpoint", callerUrl), "Bearer " + CALLER_KEY);
    }

    private void assertServerKeyNotSent(Map<String, Object> conf) {
        assertFails(db, EMBEDDING_QUERY, map("apiKey", null, "conf", conf), ERROR_UNTRUSTED_ENDPOINT);
        assertEquals(List.of(), server.getRequests());
    }

    private void assertEmbeddingSentWith(String apiKey, Map<String, Object> conf, String expectedAuthorization) {
        testCall(db, EMBEDDING_QUERY, map("apiKey", apiKey, "conf", conf),
                r -> assertEquals(List.of(0.1, 0.2), r.get("embedding")));

        List<RecordedRequest> requests = server.getRequests();
        assertEquals(1, requests.size());
        assertEquals(expectedAuthorization, requests.get(0).header("Authorization"));
    }
}
