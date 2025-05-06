package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ml.MLUtil.*;
import static apoc.ml.OpenAITestResultUtils.CHAT_COMPLETION_QUERY;
import static apoc.ml.OpenAITestResultUtils.EMBEDDING_QUERY;
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.ml.OpenAITestResultUtils.assertEmbeddings;
import static apoc.util.TestUtil.testCall;

/**
 * Tests with Mistral API: https://docs.mistral.ai/platform/endpoints/ 
 */
public class OpenAIMistralApiIT {

    private String mistralApiKey;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        mistralApiKey = System.getenv("MISTRAL_API_KEY");
        Assume.assumeNotNull("No MISTRAL_API_KEY environment configured", mistralApiKey);
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void chatCompletionWithMistralLarge() {
        chatCompletionTestCommon("mistral-large-latest");
    }

    @Test
    public void chatCompletionWithMistralMedium() {
        chatCompletionTestCommon("mistral-medium-latest");
    }

    @Test
    public void chatCompletionWithMistralSmall() {
        chatCompletionTestCommon("mistral-small-latest");
    }

    @Test
    public void chatCompletionWithMistral8x7B() {
        chatCompletionTestCommon("open-mixtral-8x7b");
    }

    @Test
    public void chatCompletionWithMistral7B() {
        chatCompletionTestCommon("open-mistral-7b");
    }

    @Test
    public void chatCompletionWithMistralEmbed() {
        String model = "mistral-embed";
        testCall(db, EMBEDDING_QUERY,
                getParams(model),
                r -> assertEmbeddings(r, 1024));
    }

    private void chatCompletionTestCommon(String model) {
        testCall(db, CHAT_COMPLETION_QUERY,
                getParams(model),
                (row) -> assertChatCompletion(row, model));
    }

    private Map<String, Object> getParams(String model) {
        Map<String, String> conf = Map.of(ENDPOINT_CONF_KEY, "https://api.mistral.ai/v1",
                MODEL_CONF_KEY, model);

        return Map.of("apiKey", mistralApiKey, "conf", conf);
    }
}
