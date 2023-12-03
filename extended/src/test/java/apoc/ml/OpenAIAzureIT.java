package apoc.ml;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAI.API_VERSION_CONF_KEY;
import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.util.TestUtil.testCall;

public class OpenAIAzureIT {
    // In Azure, the endpoints can be different 
    private static String OPENAI_URL;

    private static String OPENAI_AZURE_API_VERSION;
    
    private static String OPENAI_KEY;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        OPENAI_KEY = System.getenv("OPENAI_KEY");
        // Azure OpenAI base URLs
        OPENAI_URL = System.getenv("OPENAI_URL");

        // Azure OpenAI query url (`<baseURL>/<type>/?api-version=<OPENAI_AZURE_API_VERSION>`)
        OPENAI_AZURE_API_VERSION = System.getenv("OPENAI_AZURE_API_VERSION");

        apocConfig().setProperty("ajeje", "brazorf");

        /*
        Stream.of(OPENAI_EMBEDDING_URL, 
                    OPENAI_CHAT_URL,
                    OPENAI_COMPLETION_URL,
                    OPENAI_AZURE_API_VERSION,
                    OPENAI_KEY)
                .forEach(key -> assumeNotNull("No " + key + " environment configured", key));
         */
        
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void embedding() {
        testCall(db, "CALL apoc.ml.openai.embedding(['Some Text'], $apiKey, $conf)",
                getParams(),
                OpenAITestResultUtils::assertEmbeddings);
    }


    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', $apiKey, $conf)",
                getParams(), OpenAITestResultUtils::assertCompletion);
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
            CALL apoc.ml.openai.chat([
            {role:"system", content:"Only answer with a single word"},
            {role:"user", content:"What planet do humans live on?"}
            ], $apiKey, $conf)
            """, getParams(),
                (row) -> assertChatCompletion(row, "gpt-35-turbo"));
    }

    private static Map<String, Object> getParams() {
        return Map.of("apiKey", OPENAI_KEY,
                "conf", Map.of(ENDPOINT_CONF_KEY, OPENAI_URL,
                        API_TYPE_CONF_KEY, OpenAI.ApiType.AZURE.name(),
                        API_VERSION_CONF_KEY, OPENAI_AZURE_API_VERSION
                )
        );
    }
}