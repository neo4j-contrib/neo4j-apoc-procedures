package apoc.ml;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpenAITestResultUtils {
    public static final String EMBEDDING_QUERY = "CALL apoc.ml.openai.embedding(['Some Text'], $apiKey, $conf)";
    public static final String CHAT_COMPLETION_QUERY = """
            CALL apoc.ml.openai.chat([
            {role:"system", content:"Only answer with a single word"},
            {role:"user", content:"What planet do humans live on?"}
            ], $apiKey, $conf)
            """;
    public static final String CHAT_COMPLETION_QUERY_WITHOUT_SYSTEM = """
            CALL apoc.ml.openai.chat([
            { content: "What planet do humans live on?", role: "user" },
            { content: "Only answer with a single word", role: "assistant" }
            ], $apiKey, $conf)
            """;
    public static final String COMPLETION_QUERY = "CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', $apiKey, $conf)";

    public static final String COMPLETION_QUERY_EXTENDED_PROMPT = "CALL apoc.ml.openai.completion('\\n\\nHuman: What color is sky?\\n\\nAssistant:', $apiKey, $conf)";
    public static final String TEXT_TO_CYPHER_QUERY = """
            WITH $schema as schema, $question as question
            CALL apoc.ml.openai.chat([
                        {role:"system", content:"Given an input question, convert it to a Cypher query. No pre-amble."},
                        {role:"user", content:"Based on the Neo4j graph schema below, write a Cypher query that would answer the user's question:
            \\n "+ schema +" \\n\\n Question: "+ question +" \\n Cypher query:"}
                        ], $apiKey, $conf) YIELD value RETURN value
            """;

    public static void assertEmbeddings(Map<String, Object> row) {
        assertEmbeddings(row, 1536);
    }
    
    public static void assertEmbeddings(Map<String, Object> row, int embeddingSize) {
        assertEquals(0L, row.get("index"));
        assertEquals("Some Text", row.get("text"));
        var embedding = (List<Double>) row.get("embedding");
        assertEquals(embeddingSize, embedding.size());
    }

    public static void assertCompletion(Map<String, Object> row, String expectedModel) {
        var result = (Map<String,Object>) row.get("value");
        assertTrue(result.get("created") instanceof Number);
        assertTrue(result.containsKey("choices"));
        var finishReason = (String)((List<Map>) result.get("choices")).get(0).get("finish_reason");
        assertTrue(finishReason.matches("stop|length"));
        String text = (String) ((List<Map>) result.get("choices")).get(0).get("text");
        System.out.println("OpenAI text response for assertCompletion = " + text);
        assertTrue(text != null && !text.isBlank());
        assertTrue(text.toLowerCase().contains("blue"));
        assertTrue(result.containsKey("usage"));
        assertTrue(((Map) result.get("usage")).get("prompt_tokens") instanceof Number);
        assertEquals(expectedModel, result.get("model"));
        assertEquals("text_completion", result.get("object"));
    }

    public static String assertChatCompletion(Map<String, Object> row, String modelId) {
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

        return text;
    }
}
