package apoc.ml;

import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * Mock tests, with a localhost endpoint URL
 */
public class VertexAITest {
    private static ClientAndServer mockServer;
    
    private static final String AUTH_HEADER = "Vertex AI mocked";
    private static final String VERTEX_MOCK_FOLDER = "vertex/";
    private static final String EMBEDDINGS = "embeddings.json";
    private static final String COMPLETION = "completions.json";
    private static final String CHAT_COMPLETION = "chatCompletions.json";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void startServer() throws URISyntaxException {
        TestUtil.registerProcedure(db, VertexAI.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);

        mockServer = startClientAndServer(1080);

        var path = Paths.get(getUrlFileName(VERTEX_MOCK_FOLDER + EMBEDDINGS).toURI()).getParent().toUri();
        // %2$s will be substituted by project parameter, 
        // see String.format(urlTemplate, region, project, region, model) in VertexAi.executeRequest method
        System.setProperty(VertexAI.APOC_ML_VERTEXAI_URL, path + "%2$s");

        Stream.of(EMBEDDINGS, COMPLETION, CHAT_COMPLETION)
                .forEach(VertexAITest::setRequestResponse);
    }

    private static void setRequestResponse(String path) {
        try {
            File urlFileName = new File(getUrlFileName(VERTEX_MOCK_FOLDER + path).getFile());
            String body = FileUtils.readFileToString(urlFileName, UTF_8);

            mockServer.when(request()
                            .withPath(path)
                            .withHeader("Authorization", "Bearer " + AUTH_HEADER)
                    )
                    .respond(response()
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
        testCall(db, "CALL apoc.ml.vertexai.embedding(['Some Text'], $accessToken, $project)", 
                Map.of("accessToken", AUTH_HEADER, "project", EMBEDDINGS),
                (row) -> {
            assertEquals(0L, row.get("index"));
            assertEquals("Some Text", row.get("text"));
            assertEquals(List.of(0.0023064255, -0.009327292, -0.0028842222), row.get("embedding"));
        });
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.vertexai.completion('What color is the sky? Answer: ', $accessToken, $project)",
                Map.of("accessToken", AUTH_HEADER, "project", COMPLETION),
                (row) -> {
                    var result = (Map<String,Object>)row.get("value");
                    var safetyAttributes = (Map) result.get("safetyAttributes");
                    assertEquals(false, safetyAttributes.get("blocked"));
                    assertEquals(List.of("Violent"), safetyAttributes.get("categories"));

                    assertEquals("RESPONSE", result.get("content"));

                    assertEquals("NO_ACTION", ((Map)result.get("recitationResult")).get("recitationAction"));
        });
    }

    @Test
    public void chatCompletion() {
        testCall(db, "CALL apoc.ml.vertexai.chat([{role:'one', content:'bar'}, {role:'two', content:'foo'}], $accessToken, $project)",
                Map.of("accessToken", AUTH_HEADER, "project", CHAT_COMPLETION),
                (row) -> {
                    var result = (Map<String,Object>)row.get("value");

                    var safetyAttributes = (Map) result.get("safetyAttributes");
                    assertEquals(List.of(), safetyAttributes.get("categories"));

                    assertEquals("NO_ACTION", ((Map)result.get("recitationResult")).get("recitationAction"));

                    var candidates = (List<Map<String,Object>>)result.get("candidates");
                    Map<String, Object> candidate = candidates.get(0);
                    var author = candidate.get("author");
                    assertEquals("AUTHOR", author);
                    assertEquals("RESPONSE", candidate.get("content"));
        });
    }
}
