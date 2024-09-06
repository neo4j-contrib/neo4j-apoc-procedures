package apoc.ml;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_TYPE;
import static apoc.ExtendedApocConfig.APOC_OPENAI_KEY;
import static apoc.ml.MLUtil.*;
import static apoc.ml.RestAPIConfig.METHOD_KEY;


@Extended
public class OpenAI {
    public static final String API_TYPE_CONF_KEY = "apiType";
    public static final String APIKEY_CONF_KEY = "apiKey";
    public static final String JSON_PATH_CONF_KEY = "jsonPath";
    public static final String PATH_CONF_KEY = "path";
    public static final String GPT_4O_MODEL = "gpt-4o";

    @Context
    public ApocConfig apocConfig;

    @Context
    public URLAccessChecker urlAccessChecker;

    public static final String APOC_ML_OPENAI_URL = "apoc.ml.openai.url";

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

    static Stream<Object> executeRequest(String apiKey, Map<String, Object> configuration, String path, String model, String key, Object inputs, String jsonPath, ApocConfig apocConfig, URLAccessChecker urlAccessChecker) throws JsonProcessingException, MalformedURLException {
        apiKey = (String) configuration.getOrDefault(APIKEY_CONF_KEY, apocConfig.getString(APOC_OPENAI_KEY, apiKey));
        if (apiKey == null || apiKey.isBlank())
            throw new IllegalArgumentException("API Key must not be empty");

        String apiTypeString = (String) configuration.getOrDefault(API_TYPE_CONF_KEY,
                apocConfig.getString(APOC_ML_OPENAI_TYPE, OpenAIRequestHandler.Type.OPENAI.name())
        );
        OpenAIRequestHandler.Type type = OpenAIRequestHandler.Type.valueOf(apiTypeString.toUpperCase(Locale.ENGLISH));
        
        var configForPayload = new HashMap<>(configuration);
        // we remove these keys from configPayload, since the json payload is calculated starting from the configPayload map
        Stream.of(ENDPOINT_CONF_KEY, API_TYPE_CONF_KEY, API_VERSION_CONF_KEY, APIKEY_CONF_KEY).forEach(configForPayload::remove);

        final Map<String, Object> headers = new HashMap<>();

        handleAPIProvider(type, configuration, path, model, key, inputs, configForPayload, headers);

        path = (String) configuration.getOrDefault(PATH_CONF_KEY, path);
        OpenAIRequestHandler apiType = type.get();

        jsonPath = (String) configuration.getOrDefault(JSON_PATH_CONF_KEY, jsonPath);
        headers.put("Content-Type", "application/json");
        apiType.addApiKey(headers, apiKey);

        String payload = JsonUtil.OBJECT_MAPPER.writeValueAsString(configForPayload);
        
        // new URL(endpoint), path) can produce a wrong path, since endpoint can have for example embedding,
        // eg: https://my-resource.openai.azure.com/openai/deployments/apoc-embeddings-model
        // therefore is better to join the not-empty path pieces
        var url = apiType.getFullUrl(path, configuration, apocConfig);
        return JsonUtil.loadJson(url, headers, payload, jsonPath, true, List.of(), urlAccessChecker);
    }

    private static void handleAPIProvider(OpenAIRequestHandler.Type type,
                                          Map<String, Object> configuration,
                                          String path,
                                          String model,
                                          String key,
                                          Object inputs,
                                          HashMap<String, Object> configForPayload,
                                          Map<String, Object> headers) {
        switch (type) {
            case MIXEDBREAD_CUSTOM -> {
                // no payload manipulation, taken from the configuration as-is
            }
            case HUGGINGFACE -> {
                configForPayload.putIfAbsent("inputs", inputs);
                configuration.putIfAbsent(PATH_CONF_KEY, "");
                headers.putIfAbsent(METHOD_KEY, "POST");
                configuration.putIfAbsent(JSON_PATH_CONF_KEY, "$[0]");
            }
            case ANTHROPIC -> {
                headers.putIfAbsent(ANTHROPIC_VERSION, configuration.getOrDefault(ANTHROPIC_VERSION, "2023-06-01"));

                if (path.equals("completions")) {
                    configuration.putIfAbsent(PATH_CONF_KEY, "complete");
                    configForPayload.putIfAbsent(MAX_TOKENS_TO_SAMPLE, 1000);
                    configForPayload.putIfAbsent(MODEL_CONF_KEY, "claude-2.1");
                } else {
                    configuration.putIfAbsent(PATH_CONF_KEY, "messages");
                    configForPayload.putIfAbsent(MAX_TOKENS, 1000);
                    configForPayload.putIfAbsent(MODEL_CONF_KEY, "claude-3-5-sonnet-20240620");
                }

                configForPayload.remove(ANTHROPIC_VERSION);
                configForPayload.put(key, inputs);
            }
            default -> {
                configForPayload.putIfAbsent(MODEL_CONF_KEY, model);
                configForPayload.put(key, inputs);
            }
        }
    }

    @Procedure("apoc.ml.openai.embedding")
    @Description("apoc.openai.embedding([texts], api_key, configuration) - returns the embeddings for a given text")
    public Stream<EmbeddingResult> getEmbedding(@Name("texts") List<String> texts, @Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        // https://platform.openai.com/docs/api-reference/embeddings/create
    /*
    { "object": "list",
      "data": [
        {
          "object": "embedding",
          "embedding": [ 0.0023064255, -0.009327292, .... (1536 floats total for ada-002) -0.0028842222 ],
          "index": 0
        }
      ],
      "model": "text-embedding-ada-002",
      "usage": { "prompt_tokens": 8, "total_tokens": 8 } }
    */
        return getEmbeddingResult(texts, apiKey, configuration, apocConfig, urlAccessChecker,
                (map, text) -> {
                    Long index = (Long) map.get("index");
                    return new EmbeddingResult(index, text, (List<Double>) map.get("embedding"));
                }, 
                m -> new EmbeddingResult(-1, m, List.of())
        );
    }

    static <T> Stream<T> getEmbeddingResult(List<String> texts, String apiKey, Map<String, Object> configuration, ApocConfig apocConfig, URLAccessChecker urlAccessChecker,
                                            BiFunction<Map, String, T> embeddingMapping, Function<String, T> nullMapping) throws JsonProcessingException, MalformedURLException {
        if (texts == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        
        Map<Boolean, List<String>> collect = texts.stream()
                .collect(Collectors.groupingBy(Objects::nonNull));

        List<String> nonNullTexts = collect.get(true);

        Stream<Object> resultStream = executeRequest(apiKey, configuration, "embeddings", "text-embedding-ada-002", "input", nonNullTexts, "$.data", apocConfig, urlAccessChecker);
        Stream<T> embeddingResultStream = resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(m -> {
                    Long index = (Long) m.get("index");
                    String text = nonNullTexts.get(index.intValue());
                    return embeddingMapping.apply(m, text);
                });

        List<String> nullTexts = collect.getOrDefault(false, List.of());
        Stream<T> nullResultStream = nullTexts.stream()
                .map(nullMapping);
        return Stream.concat(embeddingResultStream, nullResultStream);
    }


    @Procedure("apoc.ml.openai.completion")
    @Description("apoc.ml.openai.completion(prompt, api_key, configuration) - prompts the completion API")
    public Stream<MapResult> completion(@Name("prompt") String prompt, @Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        // https://platform.openai.com/docs/api-reference/completions/create
    /*
    { "id": "cmpl-uqkvlQyYK7bGYrRHQ0eXlWi7",
      "object": "text_completion", "created": 1589478378, "model": "text-davinci-003",
      "choices": [ { "text": "\n\nThis is indeed a test", "index": 0, "logprobs": null, "finish_reason": "length" } ],
      "usage": { "prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12 }
    }
    */
        if (prompt == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        return executeRequest(apiKey, configuration, "completions", "gpt-3.5-turbo-instruct", "prompt", prompt, "$", apocConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v).map(MapResult::new);
    }

    @Procedure("apoc.ml.openai.chat")
    @Description("apoc.ml.openai.chat(messages, api_key, configuration]) - prompts the completion API")
    public Stream<MapResult> chatCompletion(@Name("messages") List<Map<String, Object>> messages, @Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        if (messages == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        configuration.putIfAbsent("model", GPT_4O_MODEL);
        return executeRequest(apiKey, configuration, "chat/completions", (String) configuration.get("model"), "messages", messages, "$", apocConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v).map(MapResult::new);
        // https://platform.openai.com/docs/api-reference/chat/create
    /*
    { 'id': 'chatcmpl-6p9XYPYSTTRi0xEviKjjilqrWU2Ve', 'object': 'chat.completion', 'created': 1677649420, 'model': 'gpt-3.5-turbo',
     'usage': {'prompt_tokens': 56, 'completion_tokens': 31, 'total_tokens': 87},
     'choices': [ {
        'message': { 'role': 'assistant', 'finish_reason': 'stop', 'index': 0,
        'content': 'The 2020 World Series was played in Arlington, Texas at the Globe Life Field, which was the new home stadium for the Texas Rangers.'}
      } ] }
    */
    }
}