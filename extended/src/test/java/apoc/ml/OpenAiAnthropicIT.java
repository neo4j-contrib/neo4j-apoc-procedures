package apoc.ml;

import apoc.util.Util;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Map;

import static apoc.ml.MLUtil.MODEL_CONF_KEY;
import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAIRequestHandler.Type.ANTHROPIC;
import static apoc.ml.OpenAITestResultUtils.CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM;
import static apoc.ml.OpenAITestResultUtils.COMPLETION_QUERY_EXTENDED_PROMPT;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(Enclosed.class)
public class OpenAiAnthropicIT {

    /**
     * Tests with default model or that is expected to fail due to unknown/invalid model
     */
    public static class OpenAiAnthropicDefaultIT extends OpenAiAnthropicBaseIT {

        @Override
        String getDefModel() {
            return null;
        }

        @Test
        public void completionWithAnthropic() {
            Map<String, Object> conf = Util.map(
                    API_TYPE_CONF_KEY, ANTHROPIC.name()
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
    }

    public static class OpenAiAnthropicVersion37IT extends OpenAiAnthropicBaseIT {

        public static final String claude_sonnet = "claude-3-5-sonnet-20240620";

        @Override
        String getDefModel() {
            return claude_sonnet;
        }
    }
    

}