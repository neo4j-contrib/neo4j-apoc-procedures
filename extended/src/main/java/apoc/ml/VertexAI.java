package apoc.ml;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ml.MLUtil.ERROR_NULL_INPUT;


@Extended
public class VertexAI {
    @Context
    public URLAccessChecker urlAccessChecker;

    @Context
    public ApocConfig apocConfig;

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

    private Stream<Object> executeRequest(String accessToken, String project, Map<String, Object> configuration, String defaultModel, Object inputs, Collection<String> retainConfigKeys, URLAccessChecker urlAccessChecker) throws JsonProcessingException, MalformedURLException {
        return executeRequest(accessToken, project, configuration, defaultModel, inputs, retainConfigKeys, urlAccessChecker, VertexAIHandler.Type.PREDICT);
    }
    
    private Stream<Object> executeRequest(String accessToken, String project, Map<String, Object> configuration, String defaultModel, Object inputs, Collection<String> retainConfigKeys, URLAccessChecker urlAccessChecker, 
                                                 VertexAIHandler.Type vertexAIHandlerType) throws JsonProcessingException {
        if (accessToken == null || accessToken.isBlank())
            throw new IllegalArgumentException("Access Token must not be empty");

        Map<String, Object> headers = (Map<String, Object>) configuration.getOrDefault("headers", new HashMap<>());
        headers.putIfAbsent("Content-Type", "application/json");
        headers.putIfAbsent("Accept", "application/json");
        headers.putIfAbsent("Authorization", "Bearer " + accessToken);

        VertexAIHandler vertexAIHandler = vertexAIHandlerType.get();
        Map<String, Object> data = vertexAIHandler.getBody(inputs, configuration, retainConfigKeys);
        String payload = new ObjectMapper().writeValueAsString(data);

        String url = vertexAIHandler.getFullUrl(configuration, apocConfig, defaultModel, project);
        String jsonPath = vertexAIHandler.getJsonPath();
        return JsonUtil.loadJson(url, headers, payload, jsonPath, true, List.of(), urlAccessChecker);
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

        if (texts == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        
        Map<Boolean, List<String>> collect = texts.stream()
                .collect(Collectors.groupingBy(Objects::nonNull));

        List<String> nonNullTexts = collect.get(true);
        
        Object inputs = texts.stream().map(text -> Map.of("content", text)).toList();
        Stream<Object> resultStream = executeRequest(accessToken, project, configuration, "textembedding-gecko", inputs, List.of(), urlAccessChecker);
        AtomicInteger ai = new AtomicInteger();
        Stream<EmbeddingResult> embeddingResultStream = resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(m -> {
                    Map<String, Object> embeddings = (Map<String, Object>) ((Map) m).get("embeddings");
                    int index = ai.getAndIncrement();
                    return new EmbeddingResult(index, nonNullTexts.get(index), (List<Double>) embeddings.get("values"));
                });
        
        List<String> nullTexts = collect.getOrDefault(false, List.of());
        Stream<EmbeddingResult> nullResultStream = nullTexts.stream()
                .map(text -> {
                    // null text return index -1 to indicate that are not coming from `/embeddings` RestAPI
                    return new EmbeddingResult(-1, text, List.of());
                });
        return Stream.concat(embeddingResultStream, nullResultStream);
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
        if (prompt == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        
        Object input = List.of(Map.of("prompt",prompt));
        var parameterKeys = List.of("temperature", "topK", "topP", "maxOutputTokens");
        var resultStream = executeRequest(accessToken, project, configuration, "text-bison", input, parameterKeys, urlAccessChecker);
        return resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    public static Map<String, Object> getParameters(Map<String, Object> config, Collection<String> retainKeys) {
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
        if (messages == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        Object inputs = List.of(Map.of("context",context, "examples",examples, "messages", messages));
        var parameterKeys = List.of("temperature", "topK", "topP", "maxOutputTokens");
        return executeRequest(accessToken, project, configuration, "chat-bison", inputs, parameterKeys, urlAccessChecker)
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
    
    @Procedure("apoc.ml.vertexai.stream")
    @Description("apoc.ml.vertexai.stream(contents, accessToken, project, configuration) - prompts the streaming API")
    public Stream<MapResult> stream(@Name("messages") List<Map<String, String>> contents,
                                    @Name("accessToken") String accessToken,
                                    @Name("project") String project,
                                    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var parameterKeys = List.of("temperature", "topK", "topP", "maxOutputTokens");
        
        return executeRequest(accessToken, project, configuration, "gemini-pro", contents, parameterKeys, urlAccessChecker, VertexAIHandler.Type.STREAM)
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(MapResult::new);
    }
    
    @Procedure("apoc.ml.vertexai.custom")
    @Description("apoc.ml.vertexai.custom(contents, accessToken, project, configuration) - prompts a customizable API")
    public Stream<ObjectResult> custom(@Name(value = "body") Map<String, Object> body,
                                       @Name("accessToken") String accessToken,
                                       @Name("project") String project,
                                       @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return executeRequest(accessToken, project, configuration, "gemini-pro", body, Collections.emptyList(), urlAccessChecker, VertexAIHandler.Type.CUSTOM)
                .map(ObjectResult::new);
    }
}