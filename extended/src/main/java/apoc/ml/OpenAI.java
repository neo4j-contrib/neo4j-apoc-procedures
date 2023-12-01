package apoc.ml;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.File;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import apoc.result.MapResult;

import com.fasterxml.jackson.databind.ObjectMapper;

import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_URL;
import static apoc.ExtendedApocConfig.APOC_OPENAI_KEY;
import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_AZURE_VERSION;
import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_TYPE;


@Extended
public class OpenAI {
    enum ApiType { AZURE, OPENAI }

    public static final String API_TYPE_CONF_KEY = "apiType";
    public static final String ENDPOINT_CONF_KEY = "endpoint";
    public static final String API_VERSION_CONF_KEY = "apiVersion";
    
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

    static Stream<Object> executeRequest(String apiKey, Map<String, Object> configuration, String path, String model, String key, Object inputs, String jsonPath, ApocConfig apocConfig) throws JsonProcessingException, MalformedURLException {
        apiKey = (String) configuration.getOrDefault(APOC_OPENAI_KEY, apocConfig.getString(APOC_OPENAI_KEY, apiKey));
        if (apiKey == null || apiKey.isBlank())
            throw new IllegalArgumentException("API Key must not be empty");


        String apiTypeString = (String) configuration.getOrDefault(API_TYPE_CONF_KEY,
                apocConfig.getString(APOC_ML_OPENAI_TYPE, ApiType.OPENAI.name())
        );
        ApiType apiType = ApiType.valueOf(apiTypeString);

        String endpoint = (String) configuration.get(ENDPOINT_CONF_KEY);
        
        String apiVersion;
        Map<String, Object> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        switch (apiType) {
            case AZURE -> {
                endpoint = getEndpoint(endpoint, apocConfig, "");
                apiVersion = "?api-version=" + configuration.getOrDefault(API_VERSION_CONF_KEY, apocConfig.getString(APOC_ML_OPENAI_AZURE_VERSION));
                headers.put("api-key", apiKey);
            }
            default -> {
                endpoint = getEndpoint(endpoint, apocConfig, "https://api.openai.com/v1"); 
                apiVersion = "";
                headers.put("Authorization", "Bearer " + apiKey);
            }
        }

        var config = new HashMap<>(configuration);
        // we remove these keys from config, since the json payload is calculated starting from the config map
        Stream.of(ENDPOINT_CONF_KEY, API_TYPE_CONF_KEY, API_VERSION_CONF_KEY).forEach(config::remove);
        config.putIfAbsent("model", model);
        config.put(key, inputs);

        String payload = new ObjectMapper().writeValueAsString(config);
        
        // new URL(endpoint), path) can produce a wrong path, since endpoint can have for example embedding,
        // eg: https://my-resource.openai.azure.com/openai/deployments/apoc-embeddings-model
        // therefore is better to join the not-empty path pieces
        var url = Stream.of(endpoint, path, apiVersion)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.joining(File.separator));
        return JsonUtil.loadJson(url, headers, payload, jsonPath, true, List.of());
    }

    private static String getEndpoint(String endpointConfMap, ApocConfig apocConfig, String defaultUrl) {
        if (endpointConfMap != null) {
            return endpointConfMap;
        }
        
        String apocConfUrl = apocConfig.getString(APOC_ML_OPENAI_URL, null);
        if (apocConfUrl != null) {
            return apocConfUrl;
        }

        return System.getProperty(APOC_ML_OPENAI_URL, defaultUrl);
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
        Stream<Object> resultStream = executeRequest(apiKey, configuration, "embeddings", "text-embedding-ada-002", "input", texts, "$.data", apocConfig);
        return resultStream
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(m -> {
                    Long index = (Long) m.get("index");
                    return new EmbeddingResult(index, texts.get(index.intValue()), (List<Double>) m.get("embedding"));
                });
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
        return executeRequest(apiKey, configuration, "completions", "text-davinci-003", "prompt", prompt, "$", apocConfig)
                .map(v -> (Map<String,Object>)v).map(MapResult::new);
    }

    @Procedure("apoc.ml.openai.chat")
    @Description("apoc.ml.openai.chat(messages, api_key, configuration]) - prompts the completion API")
    public Stream<MapResult> chatCompletion(@Name("messages") List<Map<String, Object>> messages, @Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return executeRequest(apiKey, configuration, "chat/completions", "gpt-3.5-turbo", "messages", messages, "$", apocConfig)
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