package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ml.OpenAI.MODEL_CONF_KEY;
import static apoc.ml.OpenAITestResultUtils.*;
import static apoc.util.TestUtil.testCall;
import static java.util.Collections.emptyMap;

public class OpenAIIT {

    private String openaiKey;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    public OpenAIIT() {
    }

    @Before
    public void setUp() throws Exception {
        openaiKey = System.getenv("OPENAI_KEY");
        Assume.assumeNotNull("No OPENAI_KEY environment configured", openaiKey);
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void getEmbedding() {
        testCall(db, EMBEDDING_QUERY, Map.of("apiKey",openaiKey, "conf", emptyMap()),
                OpenAITestResultUtils::assertEmbeddings);
    }

    @Test
    public void getEmbedding3Small() {
        Map<String, Object> conf = Map.of(MODEL_CONF_KEY, "text-embedding-3-small");
        testCall(db, EMBEDDING_QUERY, Map.of("apiKey", openaiKey, "conf", conf),
                OpenAITestResultUtils::assertEmbeddings);
    }

    @Test
    public void getEmbedding3Large() {
        Map<String, Object> conf = Map.of(MODEL_CONF_KEY, "text-embedding-3-large");
        testCall(db, EMBEDDING_QUERY, Map.of("apiKey", openaiKey, "conf", conf),
                r -> assertEmbeddings(r, 3072));
    }

    @Test
    public void getEmbedding3SmallWithDimensionsRequestParameter() {
        Map<String, Object> conf = Map.of(MODEL_CONF_KEY, "text-embedding-3-small",
                "dimensions", 256);
        testCall(db, EMBEDDING_QUERY, Map.of("apiKey", openaiKey, "conf", conf),
                r -> assertEmbeddings(r, 256));
    }

    @Test
    public void getEmbedding3LargeWithDimensionsRequestParameter() {
        Map<String, Object> conf = Map.of(MODEL_CONF_KEY, "text-embedding-3-large",
                "dimensions", 256);
        testCall(db, EMBEDDING_QUERY, Map.of("apiKey", openaiKey, "conf", conf),
                r -> assertEmbeddings(r, 256));
    }

    @Test
    public void completion() {
        testCall(db, COMPLETION_QUERY,
                Map.of("apiKey", openaiKey, "conf", emptyMap()),
                (row) -> assertCompletion(row, "gpt-3.5-turbo-instruct"));
    }

    @Test
    public void chatCompletion() {
        testCall(db, CHAT_COMPLETION_QUERY, Map.of("apiKey",openaiKey, "conf", emptyMap()),
                (row) -> assertChatCompletion(row, "gpt-3.5-turbo"));

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