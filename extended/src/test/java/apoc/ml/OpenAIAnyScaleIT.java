package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ml.MLUtil.*;
import static apoc.ml.OpenAITestResultUtils.*;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpenAIAnyScaleIT {

    private String openaiKey;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {
        openaiKey = System.getenv("OPENAI_ANYSCALE_KEY");
        Assume.assumeNotNull("No OPENAI_ANYSCALE_KEY environment configured", openaiKey);
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void getEmbedding() {
        testCall(db, EMBEDDING_QUERY,
                getParams("thenlper/gte-large"),
                row -> {
                    assertEquals(0L, row.get("index"));
                    assertEquals("Some Text", row.get("text"));
                    var embedding = (List<Double>) row.get("embedding");
                    assertEquals(1024, embedding.size());
                });
    }

    @Test
    public void completion() {
        String modelId = "Meta-Llama/Llama-Guard-7b";
        testCall(db, COMPLETION_QUERY,
                getParams(modelId),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    assertTrue(result.get("created") instanceof Number);
                    assertTrue(result.containsKey("choices"));
                    var finishReason = (String)((List<Map>) result.get("choices")).get(0).get("finish_reason");
                    assertTrue(finishReason.matches("stop|length"));
                    String text = (String) ((List<Map>) result.get("choices")).get(0).get("text");
                    assertTrue(text != null && !text.isBlank());

                    assertEquals(modelId, result.get("model"));
                });
    }

    @Test
    public void chatCompletion() {
        String modelId = "meta-llama/Llama-2-70b-chat-hf";
        testCall(db, CHAT_COMPLETION_QUERY, 
                getParams(modelId),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    assertTrue(result.get("created") instanceof Number);
                    assertTrue(result.containsKey("choices"));

                    Map message = ((List<Map<String,Map>>) result.get("choices")).get(0).get("message");
                    assertEquals("assistant", message.get("role"));
                    String text = (String) message.get("content");
                    assertTrue(text != null && !text.isBlank());

                    assertTrue(result.containsKey("usage"));
                    assertTrue(((Map) result.get("usage")).get("prompt_tokens") instanceof Number);

                    assertTrue(result.get("model").toString().startsWith(modelId));
                });
    }

    private Map<String, Object> getParams(String model) {
        return Map.of("apiKey", openaiKey,
                "conf", Map.of(ENDPOINT_CONF_KEY, "https://api.endpoints.anyscale.com/v1",
                        MODEL_CONF_KEY, model
                )
        );
    }
}