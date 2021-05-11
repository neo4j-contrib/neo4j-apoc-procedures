package apoc.redis;

import apoc.util.Util;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static io.lettuce.core.RedisURI.DEFAULT_TIMEOUT;

public class RedisConfig {
    private final Charset charset;
    private final Duration timeout;

    private final boolean autoReconnect;
    private final boolean right;

    private final Charset scriptCharset;

    public RedisConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.charset = Charset.forName((String) config.getOrDefault("charset", "UTF-8"));
        this.timeout = Duration.ofSeconds((long) config.getOrDefault("timeout", DEFAULT_TIMEOUT));
        this.scriptCharset = Charset.forName((String) config.getOrDefault("scriptCharset", "UTF-8"));
        this.autoReconnect = Util.toBoolean(config.getOrDefault("autoReconnect", true));
        this.right = Util.toBoolean(config.getOrDefault("right", true));
    }

    public boolean isRight() {
        return right;
    }

    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    public Charset getScriptCharset() {
        return scriptCharset;
    }

    public Charset getCharset() {
        return charset;
    }

    public Duration getTimeout() {
        return timeout;
    }
    
}
