package apoc.ml;

import static apoc.ApocConfig.APOC_ML_OPENAI_TYPE;
import static apoc.ApocConfig.APOC_OPENAI_KEY;
import static apoc.ml.MLUtil.APIKEY_CONF_KEY;
import static apoc.ml.MLUtil.API_TYPE_CONF_KEY;
import static apoc.ml.MLUtil.API_VERSION_CONF_KEY;
import static apoc.ml.MLUtil.ENDPOINT_CONF_KEY;
import static apoc.ml.MLUtil.ERROR_NULL_INPUT;
import static apoc.ml.MLUtil.MODEL_CONF_KEY;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.MapResult;
import apoc.util.ExtendedUtil;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Extended
public class OpenAI {
    public static final String JSON_PATH_CONF_KEY = "jsonPath";
    public static final String FAIL_ON_ERROR_CONF = "failOnError";
    public static final String ENABLE_BACK_OFF_RETRIES_CONF_KEY = "enableBackOffRetries";
    public static final String ENABLE_EXPONENTIAL_BACK_OFF_CONF_KEY = "exponentialBackoff";
    public static final String BACK_OFF_RETRIES_CONF_KEY = "backOffRetries";

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

    static Stream<Object> executeRequest(
            String apiKey,
            Map<String, Object> configuration,
            String path,
            String model,
            String key,
            Object inputs,
            String jsonPath,
            ApocConfig apocConfig)
            throws JsonProcessingException, MalformedURLException {
        apiKey = (String) configuration.getOrDefault(APIKEY_CONF_KEY, apocConfig.getString(APOC_OPENAI_KEY, apiKey));
        boolean enableBackOffRetries = Util.toBoolean(configuration.get(ENABLE_BACK_OFF_RETRIES_CONF_KEY));
        Integer backOffRetries = Util.toInteger(configuration.getOrDefault(BACK_OFF_RETRIES_CONF_KEY, 5));
        boolean exponentialBackoff = Util.toBoolean(configuration.get(ENABLE_EXPONENTIAL_BACK_OFF_CONF_KEY));
        if (apiKey == null || apiKey.isBlank()) throw new IllegalArgumentException("API Key must not be empty");
        String apiTypeString = (String) configuration.getOrDefault(
                API_TYPE_CONF_KEY, apocConfig.getString(APOC_ML_OPENAI_TYPE, OpenAIRequestHandler.Type.OPENAI.name()));
        OpenAIRequestHandler.Type type = OpenAIRequestHandler.Type.valueOf(apiTypeString.toUpperCase(Locale.ENGLISH));

        var config = new HashMap<>(configuration);
        // we remove these keys from config, since the json payload is calculated starting from the config map
        Stream.of(ENDPOINT_CONF_KEY, API_TYPE_CONF_KEY, API_VERSION_CONF_KEY, APIKEY_CONF_KEY)
                .forEach(config::remove);
        switch (type) {
            case MIXEDBREAD_CUSTOM:
                // no payload manipulation, taken from the configuration as-is
                break;
            default:
                config.putIfAbsent(MODEL_CONF_KEY, model);
                config.put(key, inputs);
        }
        OpenAIRequestHandler apiType = type.get();

        final Map<String, Object> headers = new HashMap<>();
        String sJsonPath = (String) configuration.getOrDefault(JSON_PATH_CONF_KEY, jsonPath);
        headers.put("Content-Type", "application/json");

        apiType.addApiKey(headers, apiKey);

        String payload = JsonUtil.OBJECT_MAPPER.writeValueAsString(config);

        // new URL(endpoint), path) can produce a wrong path, since endpoint can have for example embedding,
        // eg: https://my-resource.openai.azure.com/openai/deployments/apoc-embeddings-model
        // therefore is better to join the not-empty path pieces
        var url = apiType.getFullUrl(path, configuration, apocConfig);
        return ExtendedUtil.withBackOffRetries(
                () -> JsonUtil.loadJson(url, headers, payload, sJsonPath, true, List.of()),
                enableBackOffRetries,
                backOffRetries,
                exponentialBackoff,
                exception -> {
                    if (!exception.getMessage().contains("429")) throw new RuntimeException(exception);
                });
    }

    @Procedure("apoc.ml.openai.embedding")
    @Description("apoc.openai.embedding([texts], api_key, configuration) - returns the embeddings for a given text")
    public Stream<EmbeddingResult> getEmbedding(
            @Name("texts") List<String> texts,
            @Name("api_key") String apiKey,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
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
        boolean failOnError = isFailOnError(configuration);
        if (checkNullInput(texts, failOnError)) return Stream.empty();
        texts = texts.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
        if (checkEmptyInput(texts, failOnError)) return Stream.empty();
        return getEmbeddingResult(texts, apiKey, configuration, apocConfig, (map, text) -> {
            Long index = (Long) map.get("index");
            return new EmbeddingResult(index, text, (List<Double>) map.get("embedding"));
        });
    }

    public static <T> Stream<T> getEmbeddingResult(
            List<String> texts,
            String apiKey,
            Map<String, Object> configuration,
            ApocConfig apocConfig,
            BiFunction<Map, String, T> embeddingMapping)
            throws JsonProcessingException, MalformedURLException {
        Stream<Object> resultStream = executeRequest(
                apiKey, configuration, "embeddings", "text-embedding-ada-002", "input", texts, "$.data", apocConfig);

        return resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(m -> {
                    Long index = (Long) m.get("index");
                    String text = texts.get(index.intValue());
                    return embeddingMapping.apply(m, text);
                });
    }

    @Procedure("apoc.ml.openai.completion")
    @Description("apoc.ml.openai.completion(prompt, api_key, configuration) - prompts the completion API")
    public Stream<MapResult> completion(
            @Name("prompt") String prompt,
            @Name("api_key") String apiKey,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        // https://platform.openai.com/docs/api-reference/completions/create
        /*
        { "id": "cmpl-uqkvlQyYK7bGYrRHQ0eXlWi7",
          "object": "text_completion", "created": 1589478378, "model": "text-davinci-003",
          "choices": [ { "text": "\n\nThis is indeed a test", "index": 0, "logprobs": null, "finish_reason": "length" } ],
          "usage": { "prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12 }
        }
        */
        boolean failOnError = isFailOnError(configuration);
        if (checkBlankInput(prompt, failOnError)) return Stream.empty();
        return executeRequest(
                        apiKey,
                        configuration,
                        "completions",
                        "gpt-3.5-turbo-instruct",
                        "prompt",
                        prompt,
                        "$",
                        apocConfig)
                .map(v -> (Map<String, Object>) v)
                .map(MapResult::new);
    }

    @Procedure("apoc.ml.openai.chat")
    @Description("apoc.ml.openai.chat(messages, api_key, configuration]) - prompts the completion API")
    public Stream<MapResult> chatCompletion(
            @Name("messages") List<Map<String, Object>> messages,
            @Name("api_key") String apiKey,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        boolean failOnError = isFailOnError(configuration);
        if (checkNullInput(messages, failOnError)) return Stream.empty();
        messages = messages.stream().filter(MapUtils::isNotEmpty).collect(Collectors.toList());
        if (checkEmptyInput(messages, failOnError)) return Stream.empty();
        String model = (String) configuration.putIfAbsent("model", "gpt-4o");
        return executeRequest(apiKey, configuration, "chat/completions", model, "messages", messages, "$", apocConfig)
                .map(v -> (Map<String, Object>) v)
                .map(MapResult::new);
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

    private static boolean isFailOnError(Map<String, Object> configuration) {
        return Util.toBoolean(configuration.getOrDefault(FAIL_ON_ERROR_CONF, true));
    }

    static boolean checkNullInput(Object input, boolean failOnError) {
        return checkInput(failOnError, () -> Objects.isNull(input));
    }

    static boolean checkEmptyInput(Collection<?> input, boolean failOnError) {
        return checkInput(failOnError, input::isEmpty);
    }

    static boolean checkBlankInput(String input, boolean failOnError) {
        return checkInput(failOnError, () -> StringUtils.isBlank(input));
    }

    private static boolean checkInput(boolean failOnError, Supplier<Boolean> checkFunction) {
        if (checkFunction.get()) {
            if (failOnError) throw new RuntimeException(ERROR_NULL_INPUT);
            return true;
        }
        return false;
    }
}
