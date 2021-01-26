package apoc.mongodb;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class MongoDbConfig {

    private final boolean compatibleValues;
    private final boolean extractReferences;
    private final boolean idAsMap;
    private final String fieldName;

    public MongoDbConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.compatibleValues = Util.toBoolean(config.getOrDefault("compatibleValues", true));
        this.extractReferences = Util.toBoolean(config.getOrDefault("extractReferences", false));
        this.idAsMap = Util.toBoolean(config.getOrDefault("objectIdAsMap", true));
        this.fieldName = (String) config.getOrDefault("fieldName", "_id");
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

    public String getFieldName() {
        return fieldName;
    }
}
