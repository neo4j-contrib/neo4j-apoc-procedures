package apoc.ml;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ExtendedApocConfig.APOC_ML_WATSON_URL;
import static apoc.ExtendedApocConfig.APOC_ML_WATSON_PROJECT_ID;

@Extended
public class Watson {
    private static final String PROJECT_ID_KEY = "project_id";
    private static final String SPACE_ID_KEY = "space_id";
    private static final String WML_INSTANCE_CRN_KEY = "wml_instance_crn";
    private static final String MODEL_ID_KEY = "model_id";
    private static final String DEFAULT_MODEL_ID = "ibm/granite-13b-chat-v2";

    @Context
    public ApocConfig apocConfig;

    @Context
    public URLAccessChecker urlAccessChecker;
    
    public static final String ENDPOINT_CONF_KEY = "endpoint";

    @Procedure("apoc.ml.watson.chat")
    @Description("apoc.ml.watson.chat(messages, accessToken, $configuration) - prompts the completion API")
    public Stream<MapResult> chatCompletion(@Name("messages") List<Map<String, Object>> messages, @Name("accessToken") String accessToken, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        if (messages == null) {
            return Stream.of(new MapResult(null));
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
            return Stream.of(new MapResult(null));
        }
        return executeRequest(prompt, accessToken, configuration);
    }

    private Stream<MapResult> executeRequest(Object input, String accessToken, Map<String, Object> configuration) {
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
            
            String endpoint = getEndpoint(configuration);

            var config = new HashMap<>(configuration);
            config.putIfAbsent(MODEL_ID_KEY, DEFAULT_MODEL_ID);
            config.put("input", input);
            
            Map<String, Object> headers = Map.of("Content-Type", "application/json",
                    "accept", "application/json",
                    "Authorization", "Bearer " + accessToken);
            
            String payload = JsonUtil.OBJECT_MAPPER.writeValueAsString(config);

            return JsonUtil.loadJson(endpoint, headers, payload, "$", true, List.of(), urlAccessChecker)
                    .map(v -> (Map<String, Object>) v)
                    .map(MapResult::new);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public String getEndpoint(Map<String, Object> config) {
        Object remove = config.remove(ENDPOINT_CONF_KEY);
        if (remove != null) {
            return (String) remove;
        }
        return apocConfig.getString(APOC_ML_WATSON_URL, "https://eu-de.ml.cloud.ibm.com/ml/v1-beta/generation/text?version=2023-05-29");
    }

}
