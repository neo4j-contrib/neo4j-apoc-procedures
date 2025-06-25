package apoc.diff;

import apoc.util.Util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DiffConfig {
    private final boolean findById;
    private final boolean relsInBetween;
    private final Map<String, Object> boltConfig;

    public DiffConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.findById = Util.toBoolean(config.get("findById"));
        this.relsInBetween = Util.toBoolean(config.get("relsInBetween"));
        this.boltConfig = (Map<String, Object>) config.getOrDefault("boltConfig", new HashMap<>());
    }

    public boolean isFindById() {
        return findById;
    }

    public Map<String, Object> getBoltConfig() {
        return boltConfig;
    }

    public boolean isRelsInBetween() {
        return relsInBetween;
    }
}