package apoc.ml;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpenAITestResultUtils {
    public static void assertEmbeddings(Map<String, Object> row) {
        assertEquals(0L, row.get("index"));
        assertEquals("Some Text", row.get("text"));
        var embedding = (List<Double>) row.get("embedding");
        assertEquals(1536, embedding.size());
    }

    public static void assertCompletion(Map<String, Object> row) {
        var result = (Map<String,Object>) row.get("value");
        assertTrue(result.get("created") instanceof Number);
        assertTrue(result.containsKey("choices"));
        var finishReason = (String)((List<Map>) result.get("choices")).get(0).get("finish_reason");
        assertTrue(finishReason.matches("stop|length"));
        String text = (String) ((List<Map>) result.get("choices")).get(0).get("text");
        assertTrue(text != null && !text.isBlank());
        assertTrue(text.toLowerCase().contains("blue"));
        assertTrue(result.containsKey("usage"));
        assertTrue(((Map) result.get("usage")).get("prompt_tokens") instanceof Number);
        assertEquals("text-davinci-003", result.get("model"));
        assertEquals("text_completion", result.get("object"));
    }

    public static void assertChatCompletion(Map<String, Object> row, String modelId) {
        var result = (Map<String,Object>) row.get("value");
        assertTrue(result.get("created") instanceof Number);
        assertTrue(result.containsKey("choices"));

        Map message = ((List<Map<String,Map>>) result.get("choices")).get(0).get("message");
        assertEquals("assistant", message.get("role"));
        String text = (String) message.get("content");
        assertTrue(text != null && !text.isBlank());

        assertTrue(result.containsKey("usage"));
        assertTrue(((Map) result.get("usage")).get("prompt_tokens") instanceof Number);
        assertEquals("chat.completion", result.get("object"));
        assertTrue(result.get("model").toString().startsWith(modelId));
    }
}
