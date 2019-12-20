package apoc.bolt;

import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;

public class BoltConfig {

    private final boolean virtual;
    private final boolean addStatistics;
    private final boolean readOnly;
    private final boolean withRelationshipNodeProperties;

    public BoltConfig(Map<String, Object> configMap) {
        if (configMap == null) {
            configMap = Collections.emptyMap();
        }
        this.virtual = toBoolean(configMap.getOrDefault("virtual", false));
        this.addStatistics = toBoolean(configMap.getOrDefault("statistics", false));
        this.readOnly = toBoolean(configMap.getOrDefault("readOnly", true));
        this.withRelationshipNodeProperties = toBoolean(configMap.getOrDefault("withRelationshipNodeProperties", false));
    }

    public boolean isVirtual() {
        return virtual;
    }

    public boolean isAddStatistics() {
        return addStatistics;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isWithRelationshipNodeProperties() {
        return withRelationshipNodeProperties;
    }
}
