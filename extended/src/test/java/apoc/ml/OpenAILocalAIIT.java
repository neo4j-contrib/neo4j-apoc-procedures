package apoc.ml;

import apoc.util.TestUtil;
import apoc.util.Util;
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


/**
 * To start the tests, follow the instructions provided here: https://localai.io/basics/build/
 * Then, download the embedding model, as explained here: https://localai.io/models/#embeddings-bert 
 * Finally, set the env var `LOCAL_AI_URL=http://localhost:<portNumber>/v1`, default is `LOCAL_AI_URL=http://localhost:8080/v1`
 */
public class OpenAILocalAIIT {

    private String localAIUrl;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {
        localAIUrl = System.getenv("LOCAL_AI_URL");
        Assume.assumeNotNull("No LOCAL_AI_URL environment configured", localAIUrl);
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void getEmbedding() {
        testCall(db, EMBEDDING_QUERY,
                getParams("text-embedding-ada-002"),
                row -> {
                    assertEquals(0L, row.get("index"));
                    assertEquals("Some Text", row.get("text"));
                    var embedding = (List<Double>) row.get("embedding");
                    assertEquals(384, embedding.size());
                });
    }

    @Test
    public void completion() {
        testCall(db, COMPLETION_QUERY,
                getParams("ggml-gpt4all-j"),
                (row) -> assertCompletion(row, "ggml-gpt4all-j"));
    }

    @Test
    public void chatCompletion() {
        testCall(db, CHAT_COMPLETION_QUERY, 
                getParams("ggml-gpt4all-j"),
                (row) -> assertChatCompletion(row, "ggml-gpt4all-j"));
    }

    private Map<String, Object> getParams(String model) {
        return Util.map("apiKey", "x",
                "conf", Map.of(ENDPOINT_CONF_KEY, localAIUrl,
                        MODEL_CONF_KEY, model)
        );
    }
}