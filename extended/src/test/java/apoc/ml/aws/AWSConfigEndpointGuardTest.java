package apoc.ml.aws;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import apoc.util.RecordingHttpServer;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_AWS_SECRET_KEY;
import static apoc.ml.MLUtil.ENDPOINT_CONF_KEY;
import static apoc.ml.MLUtil.REGION_CONF_KEY;
import static apoc.ml.aws.AWSConfig.ERROR_UNTRUSTED_ENDPOINT;
import static apoc.ml.aws.AWSConfig.HEADERS_KEY;
import static apoc.ml.aws.AWSConfig.KEY_ID;
import static apoc.ml.aws.AWSConfig.SECRET_KEY;
import static apoc.ml.aws.BedrockInvokeConfig.MODEL;
import static apoc.ml.aws.BedrockTestUtil.BEDROCK_CUSTOM_PROC;
import static apoc.ml.aws.BedrockTestUtil.STABILITY_AI_BODY;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class AWSConfigEndpointGuardTest {
    private static final String SERVER_KEY_ID = "SERVER-KEY-ID";
    private static final String SERVER_SECRET_KEY = "server-secret-key";
    private static RecordingHttpServer server;
    private static String callerUrl;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void startServer() throws Exception {
        TestUtil.registerProcedure(db, Bedrock.class);
        server = new RecordingHttpServer("{}");
        callerUrl = "http://localhost:" + server.getPort();
    }

    @AfterClass
    public static void stopServer() {
        server.close();
    }

    @Before
    public void before() {
        server.clear();
        apocConfig().setProperty(APOC_AWS_KEY_ID, SERVER_KEY_ID);
        apocConfig().setProperty(APOC_AWS_SECRET_KEY, SERVER_SECRET_KEY);
    }

    @After
    public void after() {
        apocConfig().getConfig().clearProperty(APOC_AWS_KEY_ID);
        apocConfig().getConfig().clearProperty(APOC_AWS_SECRET_KEY);
    }

    @Test
    public void serverCredentialsAreNotSentToCallerEndpoint() {
        assertFails(db, BEDROCK_CUSTOM_PROC,
                map("body", STABILITY_AI_BODY, "conf", map(ENDPOINT_CONF_KEY, callerUrl + "/invoke")),
                ERROR_UNTRUSTED_ENDPOINT);
        assertEquals(List.of(), server.getRequests());
    }

    @Test
    public void serverCredentialsAreNotSentToHostInjectedViaRegion() {
        Map<String, Object> config = map(MODEL, "a-model", REGION_CONF_KEY, "attacker.com/#");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new BedrockInvokeConfig(config));
        assertEquals(ERROR_UNTRUSTED_ENDPOINT, e.getMessage());
    }

    @Test
    public void serverCredentialsAreUsedForServiceEndpoint() {
        assertCredentials(new BedrockInvokeConfig(map(MODEL, "a-model", REGION_CONF_KEY, "eu-west-1")), SERVER_KEY_ID, SERVER_SECRET_KEY);
        assertCredentials(new BedrockInvokeConfig(map(ENDPOINT_CONF_KEY, "https://bedrock-runtime.us-east-1.amazonaws.com/model/x/invoke")), SERVER_KEY_ID, SERVER_SECRET_KEY);
        assertCredentials(new BedrockGetModelsConfig(map()), SERVER_KEY_ID, SERVER_SECRET_KEY);
        assertCredentials(new SageMakerConfig(map(SageMakerConfig.ENDPOINT_NAME_KEY, "an-endpoint")), SERVER_KEY_ID, SERVER_SECRET_KEY);
        assertCredentials(new SageMakerConfig(map(ENDPOINT_CONF_KEY, "https://api.sagemaker.us-east-1.amazonaws.com/")), SERVER_KEY_ID, SERVER_SECRET_KEY);
    }

    @Test
    public void serverCredentialsAreNotUsedForOtherEndpoints() {
        Stream.of("https://runtime.sagemaker.us-east-1.amazonaws.com/endpoints/x/invocations",
                        "https://abc123.execute-api.us-east-1.amazonaws.com/model/x/invoke",
                        "https://bedrock-runtime.us-east-1.amazonaws.com.attacker.com/model/x/invoke",
                        "https://bedrock-runtime.us-east-1.amazonaws.com@attacker.com/model/x/invoke",
                        "https://bedrock-runtime.us-east-1.amazonaws.com:8443/model/x/invoke",
                        "http://bedrock-runtime.us-east-1.amazonaws.com/model/x/invoke")
                .forEach(endpoint -> assertThrows(endpoint, IllegalArgumentException.class,
                        () -> new BedrockInvokeConfig(map(ENDPOINT_CONF_KEY, endpoint))));
    }

    @Test
    public void callerCredentialsAreUsedForCallerEndpoint() {
        AWSConfig config = new BedrockInvokeConfig(map(ENDPOINT_CONF_KEY, callerUrl, KEY_ID, "caller-id", SECRET_KEY, "caller-secret"));
        assertCredentials(config, "caller-id", "caller-secret");
    }

    @Test
    public void callerAuthorizationHeaderIsAllowedForCallerEndpoint() {
        new BedrockInvokeConfig(map(ENDPOINT_CONF_KEY, callerUrl, HEADERS_KEY, map("Authorization", "AWS V4 mocked")));
    }

    private static void assertCredentials(AWSConfig config, String keyId, String secretKey) {
        assertEquals(keyId, config.getKeyId());
        assertEquals(secretKey, config.getSecretKey());
    }
}
