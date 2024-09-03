package apoc.ml;

import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.apache.commons.io.FileUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.ml.MLTestUtil.assertNullInputFails;
import static apoc.ml.MLUtil.*;
import static apoc.ml.VertexAIHandler.PREDICT_RESOURCE;
import static apoc.ml.VertexAIHandler.RESOURCE_CONF_KEY;
import static apoc.ml.VertexAIHandler.STREAM_RESOURCE;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class VertexAIIT {

    private String vertexAiKey;
    private String vertexAiProject;
    
    private final List<Map<String, Object>> streamContents = List.of(
            Map.of("role", "user",
                    "parts", List.of(Map.of("text", "translate book in italian"))
            )
    );

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();
    private Map<String, Object> parameters;

    public VertexAIIT() {
    }

    @Before
    public void setUp() throws Exception {
        vertexAiKey = System.getenv("VERTEXAI_KEY");
        Assume.assumeNotNull("No VERTEXAI_KEY environment configured", vertexAiKey);
        vertexAiProject = System.getenv("VERTEXAI_PROJECT");
        Assume.assumeNotNull("No VERTEXAI_PROJECT environment configured", vertexAiProject);
        TestUtil.registerProcedure(db, VertexAI.class);
        parameters = Map.of("apiKey", vertexAiKey, "project", vertexAiProject);
    }

    @Test
    public void getEmbedding() {
        testCall(db, "CALL apoc.ml.vertexai.embedding(['Some Text'], $apiKey, $project)", parameters,(row) -> {
            System.out.println("row = " + row);
            assertEquals(0L, row.get("index"));
            assertEquals("Some Text", row.get("text"));
            var embedding = (List<Double>) row.get("embedding");
            assertEquals(768, embedding.size());
            assertEquals(true, embedding.stream().allMatch(d -> d instanceof Double));
        });
    }

    @Test
    public void getEmbeddingNull() {
        testResult(db, "CALL apoc.ml.vertexai.embedding([null, 'Some Text', null, 'Other Text'], $apiKey, $project)",
                parameters,
                r -> {
                    Set<String> actual = Iterators.asSet(r.columnAs("text"));

                    Set<String> expected = new HashSet<>() {{
                        add(null); add(null); add("Some Text"); add("Other Text");
                    }};
                    assertEquals(expected, actual);
                });
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.vertexai.completion('What color is the sky? Answer in one word: ', $apiKey, $project)", parameters,(row) -> {
            assertCorrectResponse(row, "blue");
        });
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
                    CALL apoc.ml.vertexai.chat([
                    {author:"user", content:"What planet do timelords live on?"}
                    ],  $apiKey, $project, {temperature:0},
                    "Fictional universe of Doctor Who. Only answer with a single word!",
                    [{input:{content:"What planet do humans live on?"}, output:{content:"Earth"}}])""", 
                parameters,
                (row) -> assertCorrectResponse(row, "gallifrey"));
    }

    @Test
    public void stream() {
        HashMap<String, Object> params = new HashMap<>(parameters);
        params.put("contents", streamContents);
        testCall(db, "CALL apoc.ml.vertexai.stream($contents,$apiKey, $project)", 
                params, (row) -> {
            assertCorrectResponse(row, "libro");
        });
    }
    
    @Test
    public void customWithCompleteString() {
        HashMap<String, Object> params = new HashMap<>(parameters);
        params.put("contents", streamContents);
        String endpoint = "https://us-central1-aiplatform.googleapis.com/v1/projects/" + vertexAiProject + "/locations/us-central1/publishers/google/models/gemini-pro-vision:" + STREAM_RESOURCE;
        params.put(ENDPOINT_CONF_KEY, endpoint);
        testCall(db, " CALL apoc.ml.vertexai.custom({contents: $contents}, $apiKey, null, {endpoint: $endpoint})", 
                params, 
                (row) -> assertCorrectResponse(row, "libro"));
    }

    @Test
    public void customWithCompleteStringGeminiFlash() {
        customWithCompleteStringCustomModel("gemini-1.5-flash-001");
    }

    @Test
    public void customWithStringFormat() {
        HashMap<String, Object> params = new HashMap<>(parameters);
        params.put("contents", streamContents);
        String endpoint = "https://us-central1-aiplatform.googleapis.com/v1/projects/{project}/locations/us-central1/publishers/google/models/gemini-pro-vision:" + STREAM_RESOURCE;
        params.put(ENDPOINT_CONF_KEY, endpoint);
        testCall(db, "CALL apoc.ml.vertexai.custom({contents: $contents}, $apiKey, $project, {endpoint: $endpoint})",
                params,
                (row) -> assertCorrectResponse(row, "libro"));
    }

    @Test
    public void customWithGeminiVisionMultiType() throws IOException {
        String path = Thread.currentThread().getContextClassLoader().getResource("tarallo.png").getPath();

        byte[] fileContent = FileUtils.readFileToByteArray(new File(path));
        String base64Image = Base64.getEncoder().encodeToString(fileContent);

        List<Map<String, ?>> parts = List.of(
                Map.of("text", "What is this?"),
                Map.of("inlineData", Map.of(
                        "mimeType", "image/png", "data", base64Image))
        );
        List<Map<String, ?>> contents = List.of(
                Map.of("role", "user", "parts", parts)
        );
        Map<String, Object> params = new HashMap<>(parameters);
        params.put("contents", contents);
        params.put("conf", Map.of(MODEL_CONF_KEY, "gemini-pro-vision"));

        testCall(db, """
                        CALL apoc.ml.vertexai.custom({contents: $contents},
                            $apiKey,
                            $project,
                            $conf)""", 
                params, 
                (row) -> assertCorrectResponse(row, "tarall"));
    }

    @Test
    public void customWithSuffix() {
        HashMap<String, Object> params = new HashMap<>(parameters);
        params.put("contents", streamContents);

        testCall(db, "CALL apoc.ml.vertexai.custom({contents: $contents}, $apiKey, $project)", 
                params, 
                (row) -> assertCorrectResponse(row, "libro"));
    }
    
    @Test
    public void customWithCodeBison() {
        Map<String, Object> params = new HashMap<>(parameters);
        params.put("conf", Map.of(MODEL_CONF_KEY, "codechat-bison", RESOURCE_CONF_KEY, PREDICT_RESOURCE));
        
        testCall(db, """
               CALL apoc.ml.vertexai.custom({instances:
                [{messages: [{author: "user", content: "Who are you?"}]}]
               },
               $apiKey, $project, $conf)""",
                params, 
                (row) -> assertCorrectResponse(row, "language model"));
    }

    @Test
    public void customWithChatCompletion() {
        Map<String, Object> params = new HashMap<>(parameters);
        params.put("conf", Map.of(MODEL_CONF_KEY, "chat-bison", RESOURCE_CONF_KEY, PREDICT_RESOURCE));
        
        testCall(db, """
            CALL apoc.ml.vertexai.custom({instances:
                [{messages: [{author: "user", content: "What planet do human live on?"}]}]
               },
            $apiKey, $project, $conf)""",
                params, 
            (row) -> assertCorrectResponse(row, "earth"));
    }

    @Test
    public void customWithWrongHeader() {
        Map<String, String> headers = Map.of("Content-Type", "invalid",
                "Authorization", "invalid");
        
        try {
            testCall(db, """
                        CALL apoc.ml.vertexai.custom(
                            {
                             contents: $contents
                            }, $apiKey, $project, {headers: $headers})
                """, Map.of("apiKey", vertexAiKey, 
                "project", vertexAiProject, 
                "headers", headers,
        "contents", streamContents), (row) -> fail("Should fail due to 401 response"));
        } catch (RuntimeException e) {
            String errMsg = e.getMessage();
            assertTrue(errMsg.contains("Server returned HTTP response code: 401"), "Current err. message is:" + errMsg);
        }
    }

    private void assertCorrectResponse(Map<String, Object> row, String expected) {
        String stringRow = row.toString();
        assertTrue(stringRow.toLowerCase().contains(expected),
                "Actual result is: " + stringRow);
    }

    @Test
    public void embeddingsNull() {
        assertNullInputFails(db, "CALL apoc.ml.vertexai.embedding(null, $apiKey, $project)",
                parameters
        );
    }
    
    @Test
    public void completionNull() {
        assertNullInputFails(db, "CALL apoc.ml.vertexai.completion(null, $apiKey, $project)",
                parameters
        );
    }

    @Test
    public void chatCompletionNull() {
        assertNullInputFails(db, "CALL apoc.ml.vertexai.chat(null, $apiKey, $project)",
                parameters
        );
    }

    private void customWithCompleteStringCustomModel(String model) {
        HashMap<String, Object> params = new HashMap<>(parameters);
        params.put("contents", List.of(
                Map.of("role", "user",
                        "parts", List.of(Map.of("text", "translate the word 'book' in italian"))
                )
        ));

        String endpoint = "https://us-central1-aiplatform.googleapis.com/v1/projects/" + vertexAiProject + "/locations/us-central1/publishers/google/models/{model}:{resource}";
        params.put(ENDPOINT_CONF_KEY, endpoint);
        params.put(MODEL_CONF_KEY, model);
        params.put(RESOURCE_CONF_KEY, "generateContent");

        testCall(db, " CALL apoc.ml.vertexai.custom({contents: $contents}, $apiKey, null, {endpoint: $endpoint, model: $model, resource: $resource})",
                params,
                (row) -> assertCorrectResponse(row, "libro"));
    }
}