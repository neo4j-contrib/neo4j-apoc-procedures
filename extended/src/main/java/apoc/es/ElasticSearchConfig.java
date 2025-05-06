package apoc.es;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static apoc.es.ElasticSearchHandler.Version;

public class ElasticSearchConfig {
    public static final String HEADERS_KEY = "headers";
    public static final String VERSION_KEY = "version";

    private final Map<String, Object> headers;
    private final ElasticSearchHandler version;

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
        
        String versionConf = (String) config.getOrDefault(VERSION_KEY, Version.DEFAULT.name());
        this.version = Version.valueOf(versionConf).get();
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public ElasticSearchHandler getVersion() {
        return version;
    }
}
