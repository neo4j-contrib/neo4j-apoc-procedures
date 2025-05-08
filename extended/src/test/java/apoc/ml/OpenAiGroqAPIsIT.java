package apoc.ml;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static apoc.ml.MLUtil.*;
import static apoc.ml.OpenAITestResultUtils.CHAT_COMPLETION_QUERY;
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.util.TestUtil.testCall;


/**
 * Tests with Groq API: https://console.groq.com/docs/quickstart 
 */
@RunWith(Parameterized.class)
public class OpenAiGroqAPIsIT {

    private String groqApiKey;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Parameterized.Parameters(name = "chatModel: {0}")
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][] {
                // tests with model evaluated
                {"llama3-70b-8192"},
                {"gemma2-9b-it"},
                {"llama-3.3-70b-versatile"}
        });
    }

    @Parameterized.Parameter(0)
    public String chatModel;

    @Before
    public void setUp() throws Exception {
        groqApiKey = System.getenv("GROQ_API_KEY");
        Assume.assumeNotNull("No GROQ_API_KEY environment configured", groqApiKey);
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void chatCompletionWithLlama2() {
        testCall(db, CHAT_COMPLETION_QUERY,
                getParams(chatModel),
                (row) -> assertChatCompletion(row, chatModel));
    }

    private Map<String, Object> getParams(String model) {
        Map<String, String> conf = Util.map(ENDPOINT_CONF_KEY, "https://api.groq.com/openai/v1",
                MODEL_CONF_KEY, model);
        
        return Util.map("apiKey", groqApiKey, "conf", conf);
    }
}