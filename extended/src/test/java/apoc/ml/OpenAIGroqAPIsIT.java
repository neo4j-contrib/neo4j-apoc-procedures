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
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.util.TestUtil.testCall;


/**
 * Tests with Groq API: https://console.groq.com/docs/quickstart 
 */
public class OpenAIGroqAPIsIT {

    private String groqApiKey;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {
        groqApiKey = System.getenv("GROQ_API_KEY");
        Assume.assumeNotNull("No GROQ_API_KEY environment configured", groqApiKey);
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void chatCompletionWithLlama2() {
        String model = "llama2-70b-4096";
        testCall(db, CHAT_COMPLETION_QUERY,
                getParams(model),
                (row) -> assertChatCompletion(row, model));
    }

    @Test
    public void chatCompletionWithMixtral() {
        String model = "mixtral-8x7b-32768";
        testCall(db, CHAT_COMPLETION_QUERY,
                getParams(model),
                (row) -> assertChatCompletion(row, model));
    }
    
    private Map<String, Object> getParams(String model) {
        Map<String, String> conf = Map.of(ENDPOINT_CONF_KEY, "https://api.groq.com/openai/v1",
                MODEL_CONF_KEY, model);
        
        return Map.of("apiKey", groqApiKey, "conf", conf);
    }
}