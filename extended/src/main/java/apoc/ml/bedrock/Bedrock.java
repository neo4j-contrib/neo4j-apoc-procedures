package apoc.ml.bedrock;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import apoc.Description;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static apoc.ml.bedrock.BedrockConfig.JSON_PATH;
import static apoc.ml.bedrock.BedrockInvokeConfig.MODEL;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static apoc.ml.bedrock.BedrockInvokeResult.*;


public class Bedrock {
    @Context
    public URLAccessChecker urlAccessChecker;
    
    // public for testing purpose
    public static final String JURASSIC_2_ULTRA = "ai21.j2-ultra-v1";
    public static final String TITAN_EMBED_TEXT = "amazon.titan-embed-text-v1";
    public static final String ANTHROPIC_CLAUDE_V2 = "anthropic.claude-v2";
    public static final String STABILITY_STABLE_DIFFUSION_XL = "stability.stable-diffusion-xl-v0";
    
    @Procedure("apoc.ml.bedrock.list")
    public Stream<ModelItemResult> list(@Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {

        var config = new HashMap<>(configuration);
        config.putIfAbsent(JSON_PATH, "modelSummaries[*]");
        BedrockConfig conf = new BedrockGetModelsConfig(config);
        
        return executeRequestCommon(null, conf)
                .flatMap(i -> ((List<Map<String, Object>>) i).stream())
                .map(ModelItemResult::new);
    }
    
    @Procedure("apoc.ml.bedrock.custom")
    @Description("To create a customizable bedrock call")
    public Stream<MapResult> custom(@Name(value = "body") Map<String, Object> body,
                                       @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        BedrockConfig conf = new BedrockInvokeConfig(configuration);
        
        return executeRequestReturningMap(body, conf)
                .map(MapResult::new);
    }

    @Procedure("apoc.ml.bedrock.chat")
    @Description("apoc.ml.bedrock.chat(messages, $conf) - prompts the completion API")
    public Stream<MapResult> chatCompletion(
            @Name("messages") List<Map<String, Object>> messages,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {

        var config = new HashMap<>(configuration);
        config.putIfAbsent(MODEL, ANTHROPIC_CLAUDE_V2);

        BedrockInvokeConfig conf = new BedrockInvokeConfig(config);

        return messages
                .stream()
                .flatMap(message -> {
                    if (conf.isOpenAICompatible()) {
                        transformOpenAiToBedrockRequestBody(message);
                    }
                    // default body value
                    message.putIfAbsent("max_tokens_to_sample", 200);
                    
                    return executeRequestReturningMap(message, conf)
                            .map(MapResult::new);
                });
    }

    private void transformOpenAiToBedrockRequestBody(Map<String, Object> message) {
        String content = (String) message.get("content");

        content = StringUtils.prependIfMissing(content, "\n\nHuman:");
        content = StringUtils.appendIfMissing(content, "\n\nAssistant:");
        
        message.clear();
        message.put("prompt", content);
    }

    @Procedure("apoc.ml.bedrock.completion")
    @Description("apoc.ml.bedrock.completion(prompt, $conf) - prompts the completion API")
    public Stream<MapResult> completion(@Name("prompt") String prompt,
                                       @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {

        var config = new HashMap<>(configuration);
        config.putIfAbsent(MODEL, JURASSIC_2_ULTRA);
        
        BedrockConfig conf = new BedrockInvokeConfig(config);
        
        Map body = Util.map("prompt", prompt);
        return executeRequestReturningMap(body, conf)
                .map(MapResult::new);
    }
    
    @Procedure("apoc.ml.bedrock.embedding")
    @Description("apoc.ml.bedrock.embedding([texts], $configuration) - returns the embeddings for a given text")
    public Stream<Embedding> embedding(@Name(value = "texts") List<String> texts,
                                       @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        var config = new HashMap<>(configuration);
        config.putIfAbsent(MODEL, TITAN_EMBED_TEXT);

        BedrockConfig conf = new BedrockInvokeConfig(config);
        
        return texts.stream()
                .flatMap(text -> {
                    Map body = Util.map("inputText", text);

                    return executeRequestReturningMap(body, conf)
                            .map(i -> Embedding.from(i, text));
                });
        
    }

    @Procedure("apoc.ml.bedrock.image")
    public Stream<Image> image(@Name(value = "body") Map<String, Object> body,
                               @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        configuration.putIfAbsent(MODEL, STABILITY_STABLE_DIFFUSION_XL);
        configuration.putIfAbsent(JSON_PATH, "$.artifacts[0]");
        
        BedrockConfig conf = new BedrockInvokeConfig(configuration);
        
        return executeRequestReturningMap(body, conf)
                .map(Image::from);
    }
    

    private Stream<Map<String, Object>> executeRequestReturningMap(Map body, BedrockConfig config) {
        return executeRequestCommon(body, config)
                .map(i -> (Map<String, Object>) i);
    }

    private Stream<Object> executeRequestCommon(Map body, BedrockConfig conf) {
        try {
            String bodyString = null;
            if (body != null) {
                // to be used e.g to add body entries to `apoc.ml.bedrock.completion` 
                body.putAll(conf.getBody());
                bodyString = OBJECT_MAPPER.writeValueAsString(body);
            }
            
            Map<String, Object> headers = new HashMap<>(conf.getHeaders());
            headers.putIfAbsent("Content-Type", "application/json");
            headers.putIfAbsent("accept", "*/*");

            if (!headers.containsKey("Authorization")) {
                AwsSignatureV4Generator.calculateAuthorizationHeaders(conf, bodyString, headers);
            }

            return JsonUtil.loadJson(conf.getEndpoint(), headers, bodyString, conf.getJsonPath(), true, List.of(), urlAccessChecker);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
