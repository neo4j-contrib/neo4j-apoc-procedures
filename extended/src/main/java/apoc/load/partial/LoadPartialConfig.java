package apoc.load.partial;

import apoc.util.CompressionConfig;
import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class LoadPartialConfig extends CompressionConfig {

    private final Map<String, Object> headers;
    private final int archiveLimit;
    private final int bufferLimit;
    
    public LoadPartialConfig(Map<String, Object> config) {
        super(config);
        if (config == null) config = Collections.emptyMap();
        this.headers = (Map) config.getOrDefault("headers", Map.of());
        // By default, fetch first 10MB to locate ZIP entries and buffers
        this.archiveLimit = Util.toInteger(config.getOrDefault("archiveLimit", 1024*1024*10));
        this.bufferLimit = Util.toInteger(config.getOrDefault("bufferLimit", 1024*1024*10));
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public Integer getArchiveLimit() {
        return archiveLimit;
    }

    public Integer getBufferLimit() {
        return bufferLimit;
    }
}
