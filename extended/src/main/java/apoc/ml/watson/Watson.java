package apoc.ml.watson;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ExtendedApocConfig.APOC_ML_WATSON_PROJECT_ID;
import static apoc.ml.MLUtil.ERROR_NULL_INPUT;

@Extended
public class Watson {
    static final String PROJECT_ID_KEY = "project_id";
    static final String SPACE_ID_KEY = "space_id";
    static final String MODEL_ID_KEY = "model_id";
    static final String WML_INSTANCE_CRN_KEY = "wml_instance_crn";
    static final String DEFAULT_COMPLETION_MODEL_ID = "ibm/granite-13b-chat-v2";
    static final String DEFAULT_EMBEDDING_MODEL_ID = "ibm/slate-30m-english-rtrvr";


    // The version date currently used in IBM Prompt Lab endpoints (apr 2024) 2024-04-04
    static final String DEFAULT_VERSION_DATE = "2023-05-29";
    static final String DEFAULT_REGION = "eu-de";

    @Context
    public ApocConfig apocConfig;

    @Context
    public URLAccessChecker urlAccessChecker;
    

    public record EmbeddingResult(long index, String text, List<Double> embedding) {}


    
    @Procedure("apoc.ml.watson.embedding")
    @Description("apoc.ml.watson.embedding([texts], $configuration) - returns the embeddings for a given text")
    public Stream<EmbeddingResult> embedding(@Name(value = "texts") List<String> texts,
                                             @Name("accessToken") String accessToken,
                                             @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        if (texts == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        
        AtomicInteger idx = new AtomicInteger();

        return executeRequest(texts, accessToken, configuration, WatsonHandler.Type.EMBEDDING.get())
                .flatMap(v -> ((List<Map>) v.get("results")).stream())
                .map(i -> {
                    int index = idx.getAndIncrement();
                    List<Double> embedding = (List<Double>) i.get("embedding");
                    return new EmbeddingResult(index, texts.get(index), embedding);
                });
    }

    @Procedure("apoc.ml.watson.chat")
    @Description("apoc.ml.watson.chat(messages, accessToken, $configuration) - prompts the completion API")
    public Stream<MapResult> chatCompletion(@Name("messages") List<Map<String, Object>> messages, @Name("accessToken") String accessToken, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        if (messages == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        String prompt = messages.stream()
                .map(message -> {
                    Object role = message.get("role");
                    Object content = message.get("content");
                    if (role == null || content == null) {
                        throw new RuntimeException("The `messages` items must have the keys: `role` and `content`");
                    }
                    return role + ": " + content;
                })
                .collect(Collectors.joining("\n\n"));
        
        return completion(prompt, accessToken, configuration);
    }
    
    @Procedure("apoc.ml.watson.completion")
    @Description("apoc.ml.watson.completion(prompt, accessToken, $configuration) - prompts the completion API")
    public Stream<MapResult> completion(@Name("prompt") String prompt, @Name("accessToken") String accessToken, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        if (prompt == null) {
            throw new RuntimeException(ERROR_NULL_INPUT);
        }
        return executeRequest(prompt, accessToken, configuration, WatsonHandler.Type.COMPLETION.get())
                .map(MapResult::new);
    }

    private Stream<Map> executeRequest(Object input, String accessToken, Map<String, Object> configuration, WatsonHandler type) {
        try {
            // the body request has to contain space_id or project_id or wml_instance_crn,
            // in case is missing we put the project_id from apoc.conf, otherwise we throw an exception
            if (!configuration.containsKey(PROJECT_ID_KEY) && !configuration.containsKey(SPACE_ID_KEY) && !configuration.containsKey(WML_INSTANCE_CRN_KEY)) {
                String apocConfProjectId = apocConfig.getString(APOC_ML_WATSON_PROJECT_ID, null);
                if (apocConfProjectId == null) {
                    String errMessage = "The body request has none of %s, %s, and %s and the APOC config `%s` is not present.%nPlease, define one of these"
                                             .formatted(PROJECT_ID_KEY, SPACE_ID_KEY, WML_INSTANCE_CRN_KEY, APOC_ML_WATSON_PROJECT_ID);
                    throw new RuntimeException(errMessage);
                }
                configuration.put(PROJECT_ID_KEY, apocConfProjectId);
            }
            
            String endpoint = type.getEndpoint(configuration);
            
            Map<String, Object> headers = Map.of("Content-Type", "application/json",
                    "accept", "application/json",
                    "Authorization", "Bearer " + accessToken);

            Map<String, Object> payloadMap = type.getPayload(configuration, input);
            String payload = JsonUtil.OBJECT_MAPPER.writeValueAsString(payloadMap);

            return JsonUtil.loadJson(endpoint, headers, payload, "$", true, List.of(), urlAccessChecker)
                    .map(v -> (Map<String, Object>) v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
