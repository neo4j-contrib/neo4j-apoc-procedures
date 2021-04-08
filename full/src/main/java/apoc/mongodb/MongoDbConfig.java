package apoc.mongodb;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class MongoDbConfig {

    private final boolean compatibleValues;
    private final boolean extractReferences;
    private final boolean objectIdAsMap;
    private final boolean useExtendedJson;
    private final String idFieldName;
    private final Map<String, Object> query;
    private final long skip;
    private final long limit;

    public MongoDbConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.compatibleValues = Util.toBoolean(config.getOrDefault("compatibleValues", true));
        this.extractReferences = Util.toBoolean(config.getOrDefault("extractReferences", false));
        this.objectIdAsMap = Util.toBoolean(config.getOrDefault("objectIdAsMap", true));
        this.useExtendedJson = Util.toBoolean(config.getOrDefault("useExtendedJson", true));
        this.idFieldName = (String) config.getOrDefault("idFieldName", "_id");
        this.query = (Map<String, Object>) config.get("query");
// todo - togliere       this.query = (String) config.getOrDefault("query", null);
        this.skip = Util.toLong(config.getOrDefault("skip", 0));
        this.limit = Util.toLong(config.getOrDefault("limit", 0));
    }

    public boolean isCompatibleValues() {
        return compatibleValues;
    }

    public boolean isExtractReferences() {
        return extractReferences;
    }

    public boolean isObjectIdAsMap() {
        return objectIdAsMap;
    }

    public String getIdFieldName() {
        return idFieldName;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public boolean isUseExtendedJson() {
        return useExtendedJson;
    }

    public Long getSkip() {
        return skip;
    }

    public Long getLimit() {
        return limit;
    }
}
