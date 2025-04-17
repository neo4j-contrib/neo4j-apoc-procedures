package apoc.ml;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static apoc.ml.MLUtil.ANTHROPIC_VERSION;
import static apoc.ml.MLUtil.MAX_TOKENS;
import static apoc.ml.MLUtil.MAX_TOKENS_TO_SAMPLE;
import static apoc.ml.MLUtil.MODEL_CONF_KEY;
import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAIRequestHandler.Type.ANTHROPIC;
import static apoc.ml.OpenAITestResultUtils.CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM;
import static apoc.ml.OpenAITestResultUtils.COMPLETION_QUERY_EXTENDED_PROMPT;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(Parameterized.class)
public class OpenAiAnthropicIT {

    protected String anthropicApiKey;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Parameterized.Parameters(name = "chatModel: {0}, completionModel: {1}")
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][] {
                // tests with model evaluated
                {"claude-3-7-sonnet-20250219", "claude-2.1"},
                {"claude-3-5-sonnet-20240620", "claude-2"},
                {"claude-3-haiku-20240307", "claude-2"},
                // tests with default model
                {null, null}
        });
    }

    @Parameterized.Parameter(0)
    public String chatModel;

    @Parameterized.Parameter(1)
    public String completionModel;

    @Before
    public void setUp() throws Exception {
        anthropicApiKey = System.getenv("ANTHROPIC_API_KEY");
        Assume.assumeNotNull("No ANTHROPIC_API_KEY environment configured", anthropicApiKey);
        TestUtil.registerProcedure(db, OpenAI.class);
    }


    @Test
    public void chatWithAnthropic() {
        Map<String, Object> conf = Util.map(
                API_TYPE_CONF_KEY, ANTHROPIC.name(),
                MODEL_CONF_KEY, chatModel
        );
        testCall(db, CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Util.map("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    assertTrue(generatedText.toLowerCase().contains("earth"),
                            "Actual generatedText is " + generatedText);
                });
    }

    @Test
    public void chatWithImageAnthropic() throws IOException {
        String path = Thread.currentThread().getContextClassLoader().getResource("tarallo.jpeg").getPath();
        byte[] fileContent = FileUtils.readFileToByteArray(new File(path));
        String base64Image = Base64.getEncoder().encodeToString(fileContent);

        List<Map<String, ?>> contentBody = List.of(
                Util.map(
                        "type", "image",
                        "source", Util.map(
                                "type", "base64",
                                "media_type", "image/jpeg",
                                "data", base64Image
                        )
                ),
                Util.map(
                        "type", "text",
                        "text", "What is in the above image?"
                )
        );

        List<Map<String, Object>> messages = List.of(Util.map(
                "role", "user",
                "content", contentBody
        ));

        String query = "CALL apoc.ml.openai.chat($messages, $apiKey, $conf)";

        Map<String, Object> conf = Util.map(
                API_TYPE_CONF_KEY, ANTHROPIC.name(),
                MODEL_CONF_KEY, chatModel
        );
        testCall(db, query,
                Util.map( "messages", messages,"conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    Assertions.assertThat(generatedText).containsAnyOf("tarall", "bagel");
                });
    }

    @Test
    public void completionWithAnthropic() {
        Map<String, Object> conf = Util.map(
                API_TYPE_CONF_KEY, ANTHROPIC.name(),
                MODEL_CONF_KEY, completionModel
        );
        testCall(db, COMPLETION_QUERY_EXTENDED_PROMPT,
                Util.map("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var completion = (String) result.get("completion");
                    assertTrue(completion.toLowerCase().contains("blue"),
                            "Actual generatedText is " + completion);
                });
    }

    @Test
    public void completionWithAnthropicUnknownModel() {
        String modelId = "unknown";
        Map<String, Object> conf = Util.map(
                API_TYPE_CONF_KEY, ANTHROPIC.name(),
                MODEL_CONF_KEY, modelId
        );

        assertFails(
                db,
                CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Util.map("conf", conf, "apiKey", anthropicApiKey),
                "Caused by: java.io.FileNotFoundException: https://api.anthropic.com/v1/messages"
        );
    }

    @Test
    public void completionWithAnthropicNonDefaultMaxTokens() {
        Map<String, Object> conf = Util.map(
                API_TYPE_CONF_KEY, ANTHROPIC.name(),
                MODEL_CONF_KEY, completionModel,
                MAX_TOKENS_TO_SAMPLE, 1
        );
        testCall(db, COMPLETION_QUERY_EXTENDED_PROMPT,
                Util.map("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var completion = (String) result.get("completion");
                    String[] wordCount = completion.trim().toLowerCase().split(" ");
                    assertEquals(1, wordCount.length);
                });
    }


    @Test
    public void completionWithAnthropicSmallTokenSize() {
        Map<String, Object> conf = Util.map(
                API_TYPE_CONF_KEY, ANTHROPIC.name(),
                MODEL_CONF_KEY, completionModel,
                MAX_TOKENS, 1
        );

        testCall(db, CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Util.map("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    String[] wordCount = generatedText.toLowerCase().split(" ");
                    assertEquals(1, wordCount.length);
                });
    }

    @Test
    public void completionWithAnthropicCustomVersion() {
        Map<String, Object> conf = Util.map(
                API_TYPE_CONF_KEY, ANTHROPIC.name(),
                MODEL_CONF_KEY, completionModel,
                ANTHROPIC_VERSION, "2023-06-01"
        );
        testCall(db, CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Util.map("conf", conf, "apiKey", anthropicApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    var contentList = (List<Map<String, Object>>) result.get("content");
                    Map<String, Object> content = contentList.get(0);
                    String generatedText = (String) content.get("text");
                    assertTrue(generatedText.toLowerCase().contains("earth"),
                            "Actual generatedText is " + generatedText);
                });
    }

    @Test
    public void chatWithAnthropicWrongVersion() {
        Map<String, Object> conf = Util.map(
                API_TYPE_CONF_KEY, ANTHROPIC.name(),
                MODEL_CONF_KEY, chatModel,
                ANTHROPIC_VERSION, "ajeje"
        );

        assertFails(
                db,
                CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM,
                Util.map("conf", conf, "apiKey", anthropicApiKey),
                "Server returned HTTP response code: 400 for URL"
        );
    }
}