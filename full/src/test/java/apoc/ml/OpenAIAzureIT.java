package apoc.ml;

import static apoc.ml.MLUtil.API_TYPE_CONF_KEY;
import static apoc.ml.MLUtil.API_VERSION_CONF_KEY;
import static apoc.ml.MLUtil.ENDPOINT_CONF_KEY;
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.ml.OpenAITestResultUtils.assertCompletion;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assume.assumeNotNull;

import apoc.util.TestUtil;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class OpenAIAzureIT {
    // In Azure, the endpoints can be different
    private static String OPENAI_EMBEDDING_URL;
    private static String OPENAI_CHAT_URL;
    private static String OPENAI_COMPLETION_URL;

    private static String OPENAI_AZURE_API_VERSION;

    private static String OPENAI_KEY;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        OPENAI_KEY = System.getenv("OPENAI_KEY");
        // Azure OpenAI base URLs
        OPENAI_EMBEDDING_URL = System.getenv("OPENAI_EMBEDDING_URL");
        OPENAI_CHAT_URL = System.getenv("OPENAI_CHAT_URL");
        OPENAI_COMPLETION_URL = System.getenv("OPENAI_COMPLETION_URL");

        // Azure OpenAI query url (`<baseURL>/<type>/?api-version=<OPENAI_AZURE_API_VERSION>`)
        OPENAI_AZURE_API_VERSION = System.getenv("OPENAI_AZURE_API_VERSION");

        Stream.of(OPENAI_EMBEDDING_URL, OPENAI_CHAT_URL, OPENAI_COMPLETION_URL, OPENAI_AZURE_API_VERSION, OPENAI_KEY)
                .forEach(key -> assumeNotNull("No " + key + " environment configured", key));

        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void embedding() {
        testCall(
                db,
                "CALL apoc.ml.openai.embedding(['Some Text'], $apiKey, $conf)",
                getParams(OPENAI_EMBEDDING_URL),
                OpenAITestResultUtils::assertEmbeddings);
    }

    @Test
    @Ignore("It returns wrong answers sometimes")
    public void completion() {
        testCall(
                db,
                "CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', $apiKey, $conf)",
                getParams(OPENAI_CHAT_URL),
                (row) -> assertCompletion(row, "gpt-35-turbo"));
    }

    @Test
    public void chatCompletion() {
        testCall(
                db,
                "CALL apoc.ml.openai.chat([\n" + "{role:\"system\", content:\"Only answer with a single word\"},\n"
                        + "{role:\"user\", content:\"What planet do humans live on?\"}\n"
                        + "], $apiKey, $conf)",
                getParams(OPENAI_COMPLETION_URL),
                (row) -> assertChatCompletion(row, "gpt-35-turbo"));
    }

    private static Map<String, Object> getParams(String url) {
        return Map.of(
                "apiKey",
                OPENAI_KEY,
                "conf",
                Map.of(
                        ENDPOINT_CONF_KEY,
                        url,
                        API_TYPE_CONF_KEY,
                        OpenAIRequestHandler.Type.AZURE.name(),
                        API_VERSION_CONF_KEY,
                        OPENAI_AZURE_API_VERSION,
                        // on Azure is available only "gpt-35-turbo"
                        "model",
                        "gpt-35-turbo"));
    }
}
