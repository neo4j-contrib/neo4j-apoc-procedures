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
    private final Map<String, Object> project;
    private final Map<String, Object> sort;

    private final long skip;
    private final long limit;

    public MongoDbConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.compatibleValues = Util.toBoolean(config.getOrDefault("compatibleValues", true));
        this.extractReferences = Util.toBoolean(config.getOrDefault("extractReferences", false));
        this.objectIdAsMap = Util.toBoolean(config.getOrDefault("objectIdAsMap", true));
        this.useExtendedJson = Util.toBoolean(config.getOrDefault("useExtendedJson", true));
        this.idFieldName = (String) config.getOrDefault("idFieldName", "_id");
        this.query = (Map<String, Object>) config.getOrDefault("query", Collections.emptyMap());
        this.project = (Map<String, Object>) config.getOrDefault("project", Collections.emptyMap());
        this.sort = (Map<String, Object>) config.getOrDefault("sort", Collections.emptyMap());
        this.skip = (long) config.getOrDefault("skip", 0L);
        this.limit = (long) config.getOrDefault("limit", 0L);
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

    public Map<String, Object> getSort() {
        return sort;
    }

    public Map<String, Object> getProject() {
        return project;
    }

    public boolean isUseExtendedJson() {
        return useExtendedJson;
    }

    public long getSkip() {
        return skip;
    }

    public long getLimit() {
        return limit;
    }
}
