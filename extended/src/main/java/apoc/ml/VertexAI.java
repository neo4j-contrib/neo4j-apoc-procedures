package apoc.ml;

import apoc.Extended;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;


@Extended
public class VertexAI {

    // "https://${region}-aiplatform.googleapis.com/v1/projects/${project}/locations/${region}/publishers/google/models/${model}:predict"
    private static final String BASE_URL = "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict";
    public static final String APOC_ML_VERTEXAI_URL = "apoc.ml.vertexai.url";
    public static final String DEFAULT_REGION = "us-central1";

    public static class EmbeddingResult {
        public final long index;
        public final String text;
        public final List<Double> embedding;

        public EmbeddingResult(long index, String text, List<Double> embedding) {
            this.index = index;
            this.text = text;
            this.embedding = embedding;
        }
    }

    private static Stream<Object> executeRequest(String accessToken, String project, Map<String, Object> configuration, String defaultModel, Object inputs, String jsonPath, Collection<String> retainConfigKeys) throws JsonProcessingException, MalformedURLException {
        if (accessToken == null || accessToken.isBlank())
            throw new IllegalArgumentException("Access Token must not be empty");
        if (project == null || project.isBlank())
            throw new IllegalArgumentException("Project must not be empty");
        String urlTemplate = System.getProperty(APOC_ML_VERTEXAI_URL, BASE_URL);

        String model = configuration.getOrDefault("model", defaultModel).toString();
        String region = configuration.getOrDefault("region", DEFAULT_REGION).toString();
        String endpoint = String.format(urlTemplate, region, project, region, model);

        Map<String, Object> headers = Map.of(
                "Content-Type", "application/json",
                "Accept", "application/json",
                "Authorization", "Bearer " + accessToken
        );

        Map<String, Object> data = Map.of("instances", inputs, "parameters", getParameters(configuration, retainConfigKeys));
        String payload = new ObjectMapper().writeValueAsString(data);

        return JsonUtil.loadJson(endpoint, headers, payload, jsonPath, true, List.of());
    }

    @Procedure("apoc.ml.vertexai.embedding")
    @Description("apoc.vertexai.embedding([texts], accessToken, project, configuration) - returns the embeddings for a given text")
    public Stream<EmbeddingResult> getEmbedding(@Name("texts") List<String> texts, @Name("accessToken") String accessToken, @Name("project") String project,
                                                @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        // DOCS: https://cloud.google.com/vertex-ai/docs/generative-ai/embeddings/get-text-embeddings
        // POST https://us-central1-aiplatform.googleapis.com/v1/projects/PROJECT_ID/locations/us-central1/publishers/google/models/textembedding-gecko:predict
    /*
{
  "instances": [
    { "content": "TEXT"}
  ],
}

{
  "predictions": [
    {
      "embeddings": {
        "statistics": {
          "truncated": false,
          "token_count": 6
        },
        "values": [ ... ]
      }
    }
  ]
}
    */
        Object inputs = texts.stream().map(text -> Map.of("content", text)).toList();
        Stream<Object> resultStream = executeRequest(accessToken, project, configuration, "textembedding-gecko", inputs, "$.predictions", List.of());
        AtomicInteger ai = new AtomicInteger();
        return resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(m -> {
                    Map<String,Object> embeddings = (Map<String, Object>) ((Map)m).get("embeddings");
                    int index = ai.getAndIncrement();
                    return new EmbeddingResult(index, texts.get(index), (List<Double>) embeddings.get("values"));
                });
    }


    @Procedure("apoc.ml.vertexai.completion")
    @Description("apoc.ml.vertexai.completion(prompt, accessToken, project, configuration) - prompts the completion API")
    public Stream<MapResult> completion(@Name("prompt") String prompt, @Name("accessToken") String accessToken, @Name("project") String project,
                                        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
/*
POST https://us-central1-aiplatform.googleapis.com/v1/projects/PROJECT_ID/locations/us-central1/publishers/google/models/text-bison:predict
docs https://cloud.google.com/vertex-ai/docs/generative-ai/text/test-text-prompts
{
  "instances": [
    { "prompt": "PROMPT"}
  ],
  "parameters": {
    "temperature": TEMPERATURE,
    "maxOutputTokens": MAX_OUTPUT_TOKENS,
    "topP": TOP_P,
    "topK": TOP_K
  }
}
{
  "instances": [
    { "prompt": "Give me ten interview questions for the role of program manager."}
  ],
  "parameters": {
    "temperature": 0.2,
    "maxOutputTokens": 256,
    "topK": 40,
    "topP": 0.95
  }
}
{
  "predictions": [
    {
      "safetyAttributes": {
        "categories": [
          "Violent"
        ],
        "scores": [
          0.10000000149011612
        ],
        "blocked": false
      },
      "content": "RESPONSE"
    }
  ]
}
 */
        Object input = List.of(Map.of("prompt",prompt));
        var parameterKeys = List.of("temperature", "topK", "topP", "maxOutputTokens");
        var resultStream = executeRequest(accessToken, project, configuration, "text-bison", input, "$.predictions", parameterKeys);
        return resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    private static Map<String, Object> getParameters(Map<String, Object> config, Collection<String> retainKeys) {
        /*
    "temperature": TEMPERATURE,
    "maxOutputTokens": MAX_OUTPUT_TOKENS,
    "topP": TOP_P,
    "topK": TOP_K
        "temperature": 0.3,
    "maxDecodeSteps": 200,
    "topP": 0.8,
    "topK": 40


         */
        var result = new HashMap<>(Map.of("temperature", config.getOrDefault("temperature", 0.3),
                "maxOutputTokens", config.getOrDefault("maxOutputTokens", 256),
                "maxDecodeSteps", config.getOrDefault("maxDecodeSteps", 200),
                "topP", config.getOrDefault("topP", 0.8),
                "topK", config.getOrDefault("topK", 40)
        ));
        result.keySet().retainAll(retainKeys);
        return result;
    }

    @Procedure("apoc.ml.vertexai.chat")
    @Description("apoc.ml.vertexai.chat(messages, accessToken, project, configuration]) - prompts the completion API")
    public Stream<MapResult> chatCompletion(@Name("messages") List<Map<String, String>> messages,
                                            @Name("accessToken") String accessToken, @Name("project") String project,
                                            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration,
                                            @Name(value = "context",defaultValue = "") String context,
                                            @Name(value = "examples", defaultValue = "[]") List<Map<String, Map<String,String>>> examples
                                            ) throws Exception {
        Object inputs = List.of(Map.of("context",context, "examples",examples, "messages", messages));
        var parameterKeys = List.of("temperature", "topK", "topP", "maxOutputTokens");
        return executeRequest(accessToken, project, configuration, "chat-bison", inputs, "$.predictions", parameterKeys)
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(v -> (Map<String, Object>) v).map(MapResult::new);
        // POST https://us-central1-aiplatform.googleapis.com/v1/projects/PROJECT_ID/locations/us-central1/publishers/google/models/chat-bison:predict
        // DOCS https://cloud.google.com/vertex-ai/docs/generative-ai/chat/test-chat-prompts
        /*
        {
  "instances": [{
      "context":  "CONTEXT",
      "examples": [
       {
          "input": {"content": "EXAMPLE_INPUT"},
          "output": {"content": "EXAMPLE_OUTPUT"}
       }],
      "messages": [
       {
          "author": "AUTHOR",
          "content": "CONTENT",
       }],
   }],
  "parameters": {
    "temperature": TEMPERATURE,
    "maxOutputTokens": MAX_OUTPUT_TOKENS,
    "topP": TOP_P,
    "topK": TOP_K
  }
}

{
  "instances": [{
      "context":  "My name is Ned. You are my personal assistant. My favorite movies are Lord of the Rings and Hobbit.",
      "examples": [ {
          "input": {"content": "Who do you work for?"},
          "output": {"content": "I work for Ned."}
       },
       {
          "input": {"content": "What do I like?"},
          "output": {"content": "Ned likes watching movies."}
       }],
      "messages": [
       {
          "author": "user",
          "content": "Are my favorite movies based on a book series?",
       },
       {
          "author": "bot",
          "content": "Yes, your favorite movies, The Lord of the Rings and The Hobbit, are based on book series by J.R.R. Tolkien.",
       },
       {
          "author": "user",
          "content": "When where these books published?",
       }],
   }],
  "parameters": {
    "temperature": 0.3,
    "maxDecodeSteps": 200,
    "topP": 0.8,
    "topK": 40
  }
}
{
  "predictions": [
    {
      "safetyAttributes": {
        "scores": [],
        "blocked": false,
        "categories": []
      },
      "candidates": [
        {
          "author": "AUTHOR",
          "content": "RESPONSE"
        }
      ]
    }
  ]
}
         */
    }
}