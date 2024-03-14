package apoc.ml;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// TODO - maybe move to `apoc.util` package?
public class RestAPIConfig {
    public static final String HEADERS_KEY = "headers";
    public static final String METHOD_KEY = "method";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String JSON_PATH_KEY = "jsonPath";
    public static final String BODY_KEY = "body";
    
    private final Map<String, Object> headers;
    private final Map<String, Object> body;
    private final String endpoint;
    private final String jsonPath;

    public RestAPIConfig(Map<String, Object> config) {
        this(config, Map.of(), Map.of());
    }
    
    public RestAPIConfig(Map<String, Object> config, Map additionalHeaders, Map additionalBodies) {
        if (config == null) {
            config = Collections.emptyMap();
        }

        String httpMethod = (String) config.getOrDefault(METHOD_KEY, "POST");
        Map headerConf = (Map<String, Object>) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        headerConf.putIfAbsent("content-type", "application/json");
        headerConf.putIfAbsent(METHOD_KEY, httpMethod);
        additionalHeaders.forEach( (k,v)-> headerConf.putIfAbsent(k,v) );
        
        this.headers = headerConf;

        this.endpoint = getEndpoint(config);

        this.jsonPath = (String) config.get(JSON_PATH_KEY);
        Map bodyConf = (Map<String, Object>) config.getOrDefault(BODY_KEY, new HashMap<>());
        additionalBodies.forEach( (k,v)-> bodyConf.putIfAbsent(k,v) );
        this.body = bodyConf;
    }
    
    private String getEndpoint(Map<String, Object> config) {
        String endpointConfig = (String) config.get(ENDPOINT_KEY);
        if (endpointConfig == null) {
            throw new RuntimeException("Endpoint must be specified");
        }
        return endpointConfig;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public Map<String, Object> getBody() {
        return body;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getJsonPath() {
        return jsonPath;
    }
}
