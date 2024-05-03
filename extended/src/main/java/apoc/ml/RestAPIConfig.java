package apoc.ml;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// TODO - could be moved to `apoc.util` package?
public class RestAPIConfig {
    public static final String HEADERS_KEY = "headers";
    public static final String METHOD_KEY = "method";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String JSON_PATH_KEY = "jsonPath";
    public static final String BODY_KEY = "body";
    
    private Map<String, Object> headers;
    private Map body;
    private String endpoint;
    private String jsonPath;

    public RestAPIConfig(Map<String, Object> config) {
        this(config, Map.of(), Map.of());
    }
    
    public RestAPIConfig(Map<String, Object> config, Map additionalHeaders, Map additionalBodies) {
        if (config == null) {
            config = Collections.emptyMap();
        }

        String httpMethod = (String) config.getOrDefault(METHOD_KEY, "POST");

        this.headers = populateHeaders(config, additionalHeaders, httpMethod);

        this.endpoint = (String) config.get(ENDPOINT_KEY);

        this.jsonPath = (String) config.get(JSON_PATH_KEY);
        this.body = populateBody(config, additionalBodies);
    }
    
    private static Map<String, Object> populateHeaders(Map<String, Object> config, Map additionalHeaders, String httpMethod) {
        Map headerConf = (Map<String, Object>) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        headerConf.putIfAbsent("content-type", "application/json");
        headerConf.putIfAbsent(METHOD_KEY, httpMethod);
        additionalHeaders.forEach(headerConf::putIfAbsent);
        return headerConf;
    }

    private static Map populateBody(Map<String, Object> config, Map additionalBodies) {
        Map bodyConf = (Map<String, Object>) config.getOrDefault(BODY_KEY, new HashMap<>());

        // if we force body to be null, e.g. with Http GET operations that doesn't allow payloads,
        // we skip additional body addition 
        if (bodyConf != null) {
            additionalBodies.forEach(bodyConf::putIfAbsent);
        }
        return bodyConf;
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

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    public void setBody(Map<String, Object> body) {
        this.body = body;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setJsonPath(String jsonPath) {
        this.jsonPath = jsonPath;
    }
}
