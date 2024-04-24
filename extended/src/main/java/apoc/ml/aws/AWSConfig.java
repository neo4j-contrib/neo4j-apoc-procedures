package apoc.ml.aws;

import java.util.HashMap;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_AWS_SECRET_KEY;
import static apoc.ml.MLUtil.*;

public abstract class AWSConfig {

    abstract String getDefaultEndpoint(Map<String, Object> config);
    abstract String getDefaultMethod();
    
    public static final String HEADERS_KEY = "headers";
    public static final String BODY_KEY = "body";
    public static final String JSON_PATH = "jsonPath";
    public static final String SECRET_KEY = "secretKey";
    public static final String KEY_ID = "keyId";
    public static final String METHOD_KEY = "method";

    private final String keyId;
    private final String secretKey;
    private final String region;
    private final String endpoint;
    private final String method;
    private final String jsonPath;
    
    private final Map<String, Object> headers;
    private final Map<String, Object> body;
    
    protected AWSConfig(Map<String, Object> config) {
        config = config == null ? Map.of() : config;
        
        this.keyId = apocConfig().getString(APOC_AWS_KEY_ID, (String) config.get(KEY_ID));
        this.secretKey = apocConfig().getString(APOC_AWS_SECRET_KEY, (String) config.get(SECRET_KEY));
        
        this.region = (String) config.getOrDefault(REGION_CONF_KEY, "us-east-1");
        this.endpoint = getEndpoint(config, getDefaultEndpoint(config));
        
        this.method = (String) config.getOrDefault(METHOD_KEY, getDefaultMethod()); 
        this.jsonPath = (String) config.get(JSON_PATH); 
        
        this.headers = (Map<String, Object>) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        this.body = (Map<String, Object>) config.getOrDefault(BODY_KEY, new HashMap<>());
    }

    private String getEndpoint(Map<String, Object> config, String defaultEndpoint) {
        
        String endpointConfig = (String) config.get(ENDPOINT_CONF_KEY);
        if (endpointConfig != null) {
            return endpointConfig;
        }
        if (defaultEndpoint != null) {
            return defaultEndpoint;
        }
        String errMessage = String.format("An endpoint could not be retrieved.\n" +
                         "Explicit the %s config",
                ENDPOINT_CONF_KEY);
        throw new RuntimeException(errMessage);
    }

    public String getKeyId() {
        return keyId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public String getMethod() {
        return method;
    }

    public Map<String, Object> getBody() {
        return body;
    }

    public String getJsonPath() {
        return jsonPath;
    }
}
