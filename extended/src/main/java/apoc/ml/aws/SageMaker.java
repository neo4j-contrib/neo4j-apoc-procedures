package apoc.ml.aws;

import apoc.Description;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static apoc.ml.aws.AWSConfig.HEADERS_KEY;
import static apoc.ml.aws.AWSConfig.JSON_PATH;
import static apoc.ml.aws.SageMakerConfig.ENDPOINT_NAME_KEY;
import static apoc.util.JsonUtil.OBJECT_MAPPER;

public class SageMaker {

    public record EmbeddingResult(long index, String text, List<Double> embedding) {}
    
    @Procedure("apoc.ml.sagemaker.custom")
    @Description("apoc.ml.sagemaker.chat(body, $conf) - To create a customizable SageMaker call")
    public Stream<MapResult> custom(@Name(value = "body") Object body,
                                    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        AWSConfig conf = new SageMakerConfig(configuration);

        return executeRequestReturningMap(body, conf)
                .map(MapResult::new);
    }
    
    @Procedure("apoc.ml.sagemaker.chat")
    @Description("apoc.ml.sagemaker.chat(messages, $conf) - Prompts the chat completion API")
    public Stream<MapResult> chatCompletion(
            @Name("messages") List<Map<String, String>> messages,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {

        var config = new HashMap<>(configuration);
        config.putIfAbsent(ENDPOINT_NAME_KEY,  "Endpoint-Distilbart-xsum-1-1-1");
        config.putIfAbsent(HEADERS_KEY, Util.map("Content-Type", "application/x-text"));
        
        AWSConfig conf = new SageMakerConfig(config);

        return messages
                .stream()
                .flatMap(message -> {
                    // to emulate OpenAI behaviour, e.g `{content: 'text..'},
                    // otherwise we put all json message as a body (with other models)
                    Object body = message.containsKey("content") 
                            ? message.get("content")
                            : message;
                    return executeRequestReturningMap(body, conf)
                            .map(MapResult::new);
                });
    }

    @Procedure("apoc.ml.sagemaker.completion")
    @Description("apoc.ml.sagemaker.completion(prompt, $conf) - Prompts the completion API")
    public Stream<MapResult> completion(@Name("prompt") String prompt,
                                        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        var config = new HashMap<>(configuration);
        config.putIfAbsent(ENDPOINT_NAME_KEY,  "Endpoint-GPT-2-1");
        config.putIfAbsent(HEADERS_KEY,  Map.of("Content-Type", "application/x-text"));
        AWSConfig conf = new SageMakerConfig(config);

        return executeRequestReturningMap(prompt, conf)
                .map(MapResult::new);
    }

    @Procedure("apoc.ml.sagemaker.embedding")
    @Description("apoc.ml.sagemaker.embedding([texts], $configuration) - Returns the embeddings for a given text")
    public Stream<EmbeddingResult> embedding(@Name(value = "texts") List<String> texts,
                                                           @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        var config = new HashMap<>(configuration);
        config.putIfAbsent(ENDPOINT_NAME_KEY, "Endpoint-Jina-Embeddings-v2-Base-en-1");
        config.putIfAbsent(JSON_PATH, "data[*]");
        AWSConfig conf = new SageMakerConfig(config);

        List<Map<String, String>> inputs = texts.stream().map(text -> Map.of("text", text)).toList();
        Object data = Map.of("data", inputs);

        AtomicInteger idx = new AtomicInteger();
        return executeRequestCommon(data, conf)
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(i -> {
                    int index = idx.getAndIncrement();
                    return new EmbeddingResult(index, texts.get(index), (List<Double>) i.get("embedding"));
                });
    }

    private Stream<Map<String, Object>> executeRequestReturningMap(Object body, AWSConfig config) {
        return executeRequestCommon(body, config)
                .map(i -> (Map<String, Object>) i);
    }
    
    private Stream<Object> executeRequestCommon(Object body, AWSConfig conf) {
        try {
            String bodyString = body instanceof String string
                    ? string
                    : OBJECT_MAPPER.writeValueAsString(body);
            
            Map<String, Object> headers = conf.getHeaders();
            headers.putIfAbsent("Content-Type", "application/json");
            headers.putIfAbsent("accept", "*/*");

            if (!headers.containsKey("Authorization")) {
                AwsSignatureV4Generator.calculateAuthorizationHeaders(conf, bodyString, "sagemaker");
            }

            return JsonUtil.loadJson(conf.getEndpoint(), conf.getHeaders(), bodyString, conf.getJsonPath(), true, List.of());
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
}
