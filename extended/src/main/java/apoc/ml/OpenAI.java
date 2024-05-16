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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_TYPE;
import static apoc.ExtendedApocConfig.APOC_OPENAI_KEY;
import static apoc.ml.MLUtil.*;


@Extended
public class OpenAI {
    public static final String API_TYPE_CONF_KEY = "apiType";
    public static final String APIKEY_CONF_KEY = "apiKey";
    public static final String JSON_PATH_CONF_KEY = "jsonPath";
    public static final String PATH_CONF_KEY = "path";

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
        
        var config = new HashMap<>(configuration);
        // we remove these keys from config, since the json payload is calculated starting from the config map
        Stream.of(ENDPOINT_CONF_KEY, API_TYPE_CONF_KEY, API_VERSION_CONF_KEY, APIKEY_CONF_KEY).forEach(config::remove);
        
        switch (type) {
            case HUGGINGFACE -> {
                config.putIfAbsent("inputs", inputs);
                jsonPath = "$[0]";
            }
            default -> {
                config.putIfAbsent(MODEL_CONF_KEY, model);
                config.put(key, inputs);
            }
        }
        
        OpenAIRequestHandler apiType = type.get();

        jsonPath = (String) configuration.getOrDefault(JSON_PATH_CONF_KEY, jsonPath);
        path = (String) configuration.getOrDefault(PATH_CONF_KEY, path);
        
        final Map<String, Object> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        apiType.addApiKey(headers, apiKey);

        String payload = JsonUtil.OBJECT_MAPPER.writeValueAsString(config);
        
        // new URL(endpoint), path) can produce a wrong path, since endpoint can have for example embedding,
        // eg: https://my-resource.openai.azure.com/openai/deployments/apoc-embeddings-model
        // therefore is better to join the not-empty path pieces
        var url = apiType.getFullUrl(path, configuration, apocConfig);
        return JsonUtil.loadJson(url, headers, payload, jsonPath, true, List.of(), urlAccessChecker);
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
        if (texts == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        
        Map<Boolean, List<String>> collect = texts.stream()
                .collect(Collectors.groupingBy(Objects::nonNull));

        List<String> nonNullTexts = collect.get(true);

        Stream<Object> resultStream = executeRequest(apiKey, configuration, "embeddings", "text-embedding-ada-002", "input", nonNullTexts, "$.data", apocConfig, urlAccessChecker);
        Stream<EmbeddingResult> embeddingResultStream = resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(m -> {
                    Long index = (Long) m.get("index");
                    return new EmbeddingResult(index, nonNullTexts.get(index.intValue()), (List<Double>) m.get("embedding"));
                });

        List<String> nullTexts = collect.getOrDefault(false, List.of());
        Stream<EmbeddingResult> nullResultStream = nullTexts.stream()
                .map(i -> {
                    // null text return index -1 to indicate that are not coming from `/embeddings` RestAPI
                    return new EmbeddingResult(-1, i, List.of());
                });
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
        return executeRequest(apiKey, configuration, "chat/completions", "gpt-3.5-turbo", "messages", messages, "$", apocConfig, urlAccessChecker)
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