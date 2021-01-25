package apoc.mongodb;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class MongoDbGetByIdConfig {

    private final boolean compatibleValues;
    private final boolean extractReferences;
    private final boolean idAsMap;


    public MongoDbGetByIdConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.compatibleValues = Util.toBoolean(config.getOrDefault("compatibleValues", true));
        this.extractReferences = Util.toBoolean(config.getOrDefault("extractReferences", false));
        this.idAsMap = Util.toBoolean(config.getOrDefault("objectIdAsMap", true));
    }

    public boolean isCompatibleValues() {
        return compatibleValues;
    }

    public boolean isExtractReferences() {
        return extractReferences;
    }

    public boolean isIdAsMap() {
        return idAsMap;
    }
}
