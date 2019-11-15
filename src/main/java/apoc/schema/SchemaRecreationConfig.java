package apoc.schema;

import java.util.Map;

public class SchemaRecreationConfig {
    private Map<String, Object> config;

    public SchemaRecreationConfig(Map<String, Object> config) {
        this.config = config;
    }

    long createAllIndexesTimeout() {
        return (long) config.getOrDefault("createAllIndexesTimeout", 30000L);
    }

    long allConstraintsTimeout() {
        return (long) config.getOrDefault("retrieveConstraintsTimeout", 10000L);
    }

    long dropAllConstraintsTimeout() {
        return (long) config.getOrDefault("dropAllConstraintsTimeout", 30000L);
    }

    long createAllConstraintsTimeout() {
        return (long) config.getOrDefault("createAllConstraintsTimeout", 30000L);
    }

    long allIndexesTimeout() {
        return (long) config.getOrDefault("retrieveIndexesTimeout", 10000L);
    }

    long dropAllIndexesTimeout() {
        return (long) config.getOrDefault("dropAllIndexesTimeout", 30000L);
    }


}
