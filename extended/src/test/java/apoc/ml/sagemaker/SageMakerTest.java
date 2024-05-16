package apoc.ml.sagemaker;

import apoc.ml.aws.SageMaker;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.socket.PortFactory;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.ml.MLUtil.*;
import static apoc.ml.aws.AWSConfig.HEADERS_KEY;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;


public class SageMakerTest {
    private static final String EMBEDDINGS = "embeddings";
    private static final String COMPLETIONS = "completions";
    private static final String CHAT_COMPLETIONS = "chat/completions";
    private static final Pair<String, String> AUTH_HEADER = Pair.of("Authorization", "AWS V4 mocked");
    private static final int PORT = PortFactory.findFreePort();

    private static ClientAndServer mockServer;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void startServer() {
        TestUtil.registerProcedure(db, SageMaker.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);

        mockServer = startClientAndServer(PORT);

        Stream.of(EMBEDDINGS, COMPLETIONS, CHAT_COMPLETIONS)
                .forEach(SageMakerTest::setRequestResponse);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    private static void setRequestResponse(String path) {
        try {
            File urlFileName = new File(getUrlFileName(path).getFile());
            String body = FileUtils.readFileToString(urlFileName, UTF_8);

            mockServer.when(
                            request()
                                    .withPath("/" + path)
                                    .withHeader(AUTH_HEADER.getKey(), AUTH_HEADER.getValue())
                    )
                    .respond(
                            response()
                                    .withStatusCode(200)
                                    .withHeaders(
                                            new Header("Cache-Control", "private, max-age=1000"))
                                    .withBody(body)
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void getEmbedding() {
        testCall(db, "CALL apoc.ml.sagemaker.embedding(['Some Text'], $conf)", getParams(EMBEDDINGS), (row) -> {
            assertEquals(0L, row.get("index"));
            assertEquals("Some Text", row.get("text"));
            assertEquals(List.of(0.0023064255, -0.009327292, -0.0028842222), row.get("embedding"));
        });
    }

    @Test
    public void chatCompletion() {
        testCall(db, "CALL apoc.ml.sagemaker.chat([{role:'system', content:'Only answer with a single word'}], $conf)", 
                getParams(CHAT_COMPLETIONS),
                (row) -> {
            var result = (Map<String,Object>)row.get("value");
            assertTrue(result.get("created") instanceof Number);
            assertTrue(result.containsKey("choices"));

            Map message = ((List<Map<String,Map>>) result.get("choices")).get(0).get("message");
            assertEquals("assistant", message.get("role"));
            assertEquals("stop", message.get("finish_reason"));
            String text = (String) message.get("content");
            assertTrue(text != null && !text.isBlank());

            
            assertTrue(result.containsKey("usage"));
            assertTrue(((Map) result.get("usage")).get("prompt_tokens") instanceof Number);
            assertEquals("gpt-3.5-turbo-0301", result.get("model"));
            assertEquals("chat.completion", result.get("object"));
        });
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.sagemaker.completion('What color is the sky? Answer: ', $conf)",
                getParams(COMPLETIONS),
                (row) -> {
            var result = (Map<String,Object>)row.get("value");
            assertTrue(result.get("created") instanceof Number);
            assertTrue(result.containsKey("choices"));
            assertEquals("stop", ((List<Map>)result.get("choices")).get(0).get("finish_reason"));
            String text = (String) ((List<Map>) result.get("choices")).get(0).get("text");
            assertTrue(text != null && !text.isBlank());
            assertTrue(result.containsKey("usage"));
            assertTrue(((Map) result.get("usage")).get("prompt_tokens") instanceof Number);
            assertEquals("text-davinci-003", result.get("model"));
            assertEquals("text_completion", result.get("object"));
        });
    }

    private static Map<String, Object> getParams(String path) {
        Map authHeader = Map.of(AUTH_HEADER.getKey(), AUTH_HEADER.getValue());
        return Map.of("conf", Map.of(ENDPOINT_CONF_KEY, "http://localhost:%s/%s".formatted( PORT, path),
                        HEADERS_KEY, authHeader)
        );
    }
}
