package apoc.es;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ElasticSearchConfig {
    public static final String HEADERS_KEY = "headers";

    private final Map<String, Object> headers;

    public ElasticSearchConfig(Map<String, Object> config) {
        this(config, null);
    }

    public ElasticSearchConfig(Map<String, Object> config, String httpMethod) {
        if (config == null) {
            config = Collections.emptyMap();
        }

        Map<String, Object> headerConf = (Map<String, Object>) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        headerConf.putIfAbsent("content-type", "application/json");
        if (httpMethod != null) {
            headerConf.putIfAbsent("method", httpMethod);
        }
        this.headers = headerConf;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }
}
