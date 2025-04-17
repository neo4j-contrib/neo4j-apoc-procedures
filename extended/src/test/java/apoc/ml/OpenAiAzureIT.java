package apoc.ml;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAI.PATH_CONF_KEY;
import static apoc.ml.MLUtil.*;
import static apoc.ml.OpenAITestResultUtils.CHAT_COMPLETION_QUERY;
import static apoc.ml.OpenAITestResultUtils.COMPLETION_QUERY;
import static apoc.ml.OpenAITestResultUtils.EMBEDDING_QUERY;
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.ml.OpenAITestResultUtils.assertCompletion;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assume.assumeNotNull;

@RunWith(Parameterized.class)
public class OpenAiAzureIT {
    // In Azure, the endpoints can be different 
    private static String OPENAI_EMBEDDING_URL;
    private static String OPENAI_CHAT_URL;
    private static String OPENAI_COMPLETION_URL;

    private static String OPENAI_AZURE_API_VERSION;
    
    private static String OPENAI_KEY;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @Parameterized.Parameters(name = "chatModel: {0}")
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][] {
                // tests with model evaluated
                {"gpt-35-turbo"},
                {"gpt-4.1"},
                {"gpt-4o"},
                // tests with default model
                {null}
        });
    }

    @Parameterized.Parameter(0)
    public String chatModel;

    @BeforeClass
    public static void setUp() throws Exception {
        OPENAI_KEY = System.getenv("OPENAI_AZURE_KEY");
        // Azure OpenAI base URLs
        OPENAI_EMBEDDING_URL = System.getenv("OPENAI_AZURE_EMBEDDING_URL");
        OPENAI_CHAT_URL = System.getenv("OPENAI_AZURE_CHAT_URL");
        OPENAI_COMPLETION_URL = System.getenv("OPENAI_AZURE_COMPLETION_URL");

        // Azure OpenAI query url (`<baseURL>/<type>/?api-version=<OPENAI_AZURE_API_VERSION>`)
        OPENAI_AZURE_API_VERSION = System.getenv("OPENAI_AZURE_API_VERSION");

        Stream.of(OPENAI_EMBEDDING_URL, 
                    OPENAI_CHAT_URL,
                    OPENAI_COMPLETION_URL,
                    OPENAI_AZURE_API_VERSION,
                    OPENAI_KEY)
                .forEach(key -> assumeNotNull("No " + key + " environment configured", key));
         
        
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void embedding() {
        testCall(db, EMBEDDING_QUERY,
                getParams(OPENAI_EMBEDDING_URL),
                OpenAITestResultUtils::assertEmbeddings);
    }

    @Test
    public void embeddingFixPath() {
        Map<String, Object> params = Util.map("apiKey", OPENAI_KEY,
                "conf", Util.map(ENDPOINT_CONF_KEY, OPENAI_EMBEDDING_URL,
                        API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.AZURE.name(),
                        API_VERSION_CONF_KEY, OPENAI_AZURE_API_VERSION,
                        PATH_CONF_KEY, "openai/deployments/text-embedding-ada-002/embeddings"
                ));
        testCall(db, EMBEDDING_QUERY,
                params,
                OpenAITestResultUtils::assertEmbeddings);
    }


    @Test
    @Ignore("It returns wrong answers sometimes")
    public void completion() {
        testCall(db, COMPLETION_QUERY,
                getParams(OPENAI_CHAT_URL),
                (row) -> assertCompletion(row, chatModel));
    }

    @Test
    public void chatCompletion() {
        testCall(db, CHAT_COMPLETION_QUERY, getParams(OPENAI_COMPLETION_URL),
                (row) -> assertChatCompletion(row, chatModel));
    }

    private Map<String, Object> getParams(String url) {
        return Util.map("apiKey", OPENAI_KEY,
                "conf", Util.map(ENDPOINT_CONF_KEY, url,
                        API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.AZURE.name(),
                        API_VERSION_CONF_KEY, OPENAI_AZURE_API_VERSION,
                        // on Azure is available only "gpt-35-turbo"
                        "model",chatModel
                )
        );
    }
}