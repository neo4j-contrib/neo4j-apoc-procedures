package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OpenAITest {

    private String openaiKey;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    public OpenAITest() {
    }

    @Before
    public void setUp() throws Exception {
        // openaiKey = System.getenv("OPENAI_KEY");
        // Assume.assumeNotNull("No OPENAI_KEY environment configured", openaiKey);
        var path = Paths.get(getUrlFileName("embeddings").toURI()).getParent().toUri();
        System.setProperty(OpenAI.APOC_ML_OPENAI_URL, path.toString());
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void getEmbedding() {
        testCall(db, "CALL apoc.ml.openai.embedding(['Some Text'], 'fake-api-key')", (row) -> {
            assertEquals(0L, row.get("index"));
            assertEquals("Some Text", row.get("text"));
            assertEquals(List.of(0.0023064255, -0.009327292, -0.0028842222), row.get("embedding"));
        });
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.openai.completion('What color is the sky? Answer: ', 'fake-api-key')", (row) -> {
            var result = (Map<String,Object>)row.get("value");
            assertEquals(true, result.get("created") instanceof Number);
            assertEquals(true, result.containsKey("choices"));
            assertEquals("stop", ((List<Map>)result.get("choices")).get(0).get("finish_reason"));
            String text = (String) ((List<Map>) result.get("choices")).get(0).get("text");
            assertEquals(true, text != null && !text.isBlank());
            assertEquals(true, result.containsKey("usage"));
            assertEquals(true, ((Map)result.get("usage")).get("prompt_tokens") instanceof Number);
            assertEquals("text-davinci-003", result.get("model"));
            assertEquals("text_completion", result.get("object"));
        });
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
CALL apoc.ml.openai.chat([
{role:"system", content:"Only answer with a single word"},
{role:"user", content:"What planet do humans live on?"}
], 'fake-api-key')
""", (row) -> {
            var result = (Map<String,Object>)row.get("value");
            assertEquals(true, result.get("created") instanceof Number);
            assertEquals(true, result.containsKey("choices"));

            Map message = ((List<Map<String,Map>>) result.get("choices")).get(0).get("message");
            assertEquals("assistant", message.get("role"));
            assertEquals("stop", message.get("finish_reason"));
            String text = (String) message.get("content");
            assertEquals(true, text != null && !text.isBlank());


            assertEquals(true, result.containsKey("usage"));
            assertEquals(true, ((Map)result.get("usage")).get("prompt_tokens") instanceof Number);
            assertEquals("gpt-3.5-turbo-0301", result.get("model"));
            assertEquals("chat.completion", result.get("object"));
        });

        /*
        {
  "id": "chatcmpl-6p9XYPYSTTRi0xEviKjjilqrWU2Ve",
  "object": "chat.completion",
  "created": 1677649420,
  "model": "gpt-3.5-turbo",
  "usage": {
    "prompt_tokens": 56,
    "completion_tokens": 31,
    "total_tokens": 87
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "finish_reason": "stop",
        "index": 0,
        "content": "The 2020 World Series was played in Arlington, Texas at the Globe Life Field, which was the new home stadium for the Texas Rangers."
      }
    }
  ]
}
         */
    }
}