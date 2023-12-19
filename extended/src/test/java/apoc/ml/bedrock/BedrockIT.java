package apoc.ml.bedrock;

import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_AWS_SECRET_KEY;
import static apoc.ml.bedrock.BedrockConfig.KEY_ID;
import static apoc.ml.bedrock.BedrockConfig.METHOD_KEY;
import static apoc.ml.bedrock.BedrockConfig.SECRET_KEY;
import static apoc.ml.bedrock.BedrockGetModelsConfig.*;
import static apoc.ml.bedrock.BedrockInvokeConfig.MODEL;
import static apoc.ml.bedrock.BedrockInvokeConfig.OPEN_AI_COMPATIBLE;
import static apoc.ml.bedrock.BedrockTestUtil.*;
import static apoc.ml.bedrock.BedrockUtil.*;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;


public class BedrockIT {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static String keyId;
    private static String secretKey;
    
    @BeforeClass
    public static void setUp() throws Exception {
        String keyIdEnv = "AWS_KEY_ID";
        String secretKeyEnv = "AWS_SECRET_KEY";
        
        keyId = System.getenv(keyIdEnv);
        secretKey = System.getenv(secretKeyEnv);
        assumeNotNull(keyIdEnv + " environment not configured", keyId);
        assumeNotNull(secretKeyEnv + "  environment configured", secretKey);
        
        TestUtil.registerProcedure(db, Bedrock.class);
    }
    
    @Before
    public void before() throws Exception {
        apocConfig().setProperty(APOC_AWS_KEY_ID, keyId);
        apocConfig().setProperty(APOC_AWS_SECRET_KEY, secretKey);
    }
    
    @Test
    public void testCustomWithTitanEmbedding() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", TITAN_BODY,
                        "conf", Map.of(MODEL, TITAN_EMBED_TEXT)
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    assertionsTitanEmbed(value);
                });
    }

    @Test
    public void testAuthViaConfigMap() {
        apocConfig().getConfig().clearProperty(APOC_AWS_KEY_ID);
        apocConfig().getConfig().clearProperty(APOC_AWS_SECRET_KEY);
        
        // check apocConfig correctly cleared, i.e. auth error
        try {
            testCall(db, BEDROCK_CUSTOM_PROC,
                    Map.of("body", TITAN_BODY,
                            "conf", Map.of(MODEL, TITAN_EMBED_TEXT)
                    ),
                    r -> {
                        Map value = (Map) r.get("value");
                        assertionsTitanEmbed(value);
                    });
        } catch (Exception e) {
            String msg = e.getMessage();
            assertTrue("Actual error message is: " + msg, 
                    msg.contains("Server returned HTTP response code: 403"));
        }
        
        // check that with auth as a conf map it should work
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", TITAN_BODY,
                        "conf", Map.of(MODEL, TITAN_EMBED_TEXT,
                                KEY_ID, keyId,
                                SECRET_KEY, secretKey)
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    assertionsTitanEmbed(value);
                });
        
    }
    
    @Test
    public void testCustomWithJurassicUltra() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", JURASSIC_BODY,
                        "conf", Map.of(MODEL, JURASSIC_2_ULTRA)
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    assertNotNull(value.get("completions"));
                });
    }

    @Test
    public void testCustomWithAnthropicClaude() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", ANTHROPIC_CLAUDE_CUSTOM_BODY,
                        "conf", Map.of(MODEL, "anthropic.claude-v1")
                ),
        r -> {
            Map value = (Map) r.get("value");
            assertNotNull(value.get("completion"));
            assertNotNull(value.get("stop_reason"));
        });
    }
    
    @Test
    public void testCustomWithJurassicMid() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", JURASSIC_BODY,
                        "conf", Map.of(MODEL, "ai21.j2-mid-v1")
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    assertNotNull(value.get("completions"));
                });
    }
    
    @Test
    public void testCustomWithStability() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", STABILITY_AI_BODY,
                        "conf", Map.of(MODEL, STABILITY_STABLE_DIFFUSION_XL)
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    List<Map> artifacts = (List<Map>) value.get("artifacts");
                    String base64Image = (String) artifacts.get(0).get("base64");
                    assertTrue(Base64.isBase64(base64Image));
                });
    }


    @Test
    public void testGetModelInvocationWithNullBody() {
        Map<String, String> conf = Map.of(
                "endpoint", "https://bedrock.us-east-1.amazonaws.com/logging/modelinvocations",
                METHOD_KEY, "GET");

        testCall(db, "CALL apoc.ml.bedrock.custom(null, $conf)",
                Map.of("conf", conf),
                r -> {
                    Map value = (Map) r.get("value");
                    assertTrue(value.containsKey("loggingConfig"));
                });
    }

    @Test
    public void testWrongMethod() {
        try {
            Map<String, String> conf = Map.of(
                    "endpoint", "https://bedrock.us-east-1.amazonaws.com/logging/modelinvocations",
                    METHOD_KEY, "POST");

            testCall(db, "CALL apoc.ml.bedrock.custom(null, $conf)",
                    Map.of( "conf", conf),
                    r -> fail());
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Actual message is: "+ message, message.contains("Server returned HTTP response code: 403"));
        }
    }

    @Test
    public void testStability() {
        testCall(db, "CALL apoc.ml.bedrock.image($body)",
                Map.of("body", STABILITY_AI_BODY),
                r -> {
                    String base64Image = (String) r.get("base64Image");
                    assertTrue(Base64.isBase64(base64Image));
                });
    }

    @Test
    public void testChatCompletionWithOpenAICompatibleTrue() {
        testResult(db, """
                        CALL apoc.ml.bedrock.chat([
                            {role:"system", content:"Only answer with a single word"}
                            ,{role:"user", content:"What planet do humans live on?"}
                        ], $conf)""",
                Map.of("conf", Map.of(OPEN_AI_COMPATIBLE, true)),
                this::chatCompletionAssertions);
    }

    @Test
    public void testChatCompletionWithOpenAICompatibleFalse() {
        List<Map<String, String>> body = List.of(
                Map.of("prompt", "\n\nHuman: Hello world\n\nAssistant:"),
                Map.of("prompt", "\n\nHuman: Ciao mondo\n\nAssistant:")
        );
        testResult(db, "CALL apoc.ml.bedrock.chat($body)",
                Map.of("body", body),
                this::chatCompletionAssertions);
    }

    private void chatCompletionAssertions(Result r) {
        List<Map<String, Object>> values = Iterators.asList(r.columnAs("value"));
        assertEquals(2, values.size());
        values.forEach(row -> {
            assertNotNull(row.get("completion"));
            assertNotNull(row.get("stop_reason"));
        });
    }

    @Test
    public void testCompletion() {
        testCall(db, "CALL apoc.ml.bedrock.completion('What color is the sky? Answer in one word: ')",
                row -> {
                    Map value = (Map) row.get("value");
                    List<Map> completions = (List<Map>) value.get("completions");
                    Map completion = completions.get(0);
                    Map data = (Map) completion.get("data");
                    String text = (String) data.get("text");
                    assertTrue("Actual text is: " + text, text.contains("Blue"));
                    assertNotNull(data.get("tokens"));
                    Map finishReason = (Map) completion.get("finishReason");
                    assertEquals("endoftext", finishReason.get("reason"));
                    assertNotNull(value.get("prompt"));
                });
    }

    @Test
    public void testEmbedding() {
        testCall(db, "CALL apoc.ml.bedrock.embedding($body)",
                Map.of("body", List.of(TITAN_CONTENT)),
                BedrockIT::assertionsTitanEmbed);
    }

    @Test
    public void testWrongRegion() {
        try {
            testCall(db, "CALL apoc.ml.bedrock.embedding($body, {region: 'notExistent'})",
                    Map.of("body", List.of(TITAN_CONTENT)),
                    r -> fail());
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("curr message is: " + message, message.contains("java.net.UnknownHostException"));
        }
    }
    
    @Test
    public void testGetModel() {
        testResult(db, "CALL apoc.ml.bedrock.list",
                r -> {
                    r.forEachRemaining(row -> {
                        String modelArn = (String) row.get("modelArn");
                        assertTrue(modelArn.contains("arn:aws:bedrock"));
                    });
                });

        testResult(db, "CALL apoc.ml.bedrock.list($conf)",
                Map.of("conf", Map.of(PATH_GET, "custom-models")),
                r -> {
                    r.forEachRemaining(row -> {
                        String modelArn = (String) row.get("modelArn");
                        assertTrue(modelArn.contains("arn:aws:bedrock"));
                    });
                });
    }

    private static void assertionsTitanEmbed(Map value) {
        assertNotNull(value.get("inputTextTokenCount"));
        assertNotNull(value.get("embedding"));
    }
}
