package apoc.export.parquet;

import apoc.util.MissingDependencyException;
import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class ParquetConfig {
    public static final String PARQUET_MISSING_DEPS_ERROR = """
        Cannot find the Hadoop jar (required by Parquet procedures).
        Please put the apoc-hadoop-dependencies-5.x.x-all.jar into plugin folder.
        See the documentation: https://neo4j.com/labs/apoc/5/export/parquet/#_library_requirements""";

    private final int batchSize;

    private final Map<String, Object> config;
    private final Map<String, Object> mapping;

    public ParquetConfig(Map<String, Object> config) {
        if (!Util.classExists("org.apache.parquet.schema.MessageType")) {
            throw new MissingDependencyException(PARQUET_MISSING_DEPS_ERROR);
        }
        
        this.config = config == null ? Collections.emptyMap() : config;
        this.batchSize = Util.toInteger(this.config.getOrDefault("batchSize", 20000));
        this.mapping = (Map<String, Object>) this.config.getOrDefault("mapping", Map.of());
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public Map<String, Object> getMapping() {
        return mapping;
    }
}

