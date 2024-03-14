package apoc.vectordb;

import apoc.ml.RestAPIConfig;

import java.util.Map;

public class VectorEmbeddingConfig extends RestAPIConfig {
    public static final String EMBEDDING_KEY = "embeddingKey";
    public static final String METADATA_KEY = "metadataKey";
    public static final String SCORE_KEY = "scoreKey";
    public static final String TEXT_KEY = "textKey";
    public static final String ID_KEY = "idKey";
    public static final String MAPPING_KEY = "mapping";
    
    public static final String DEFAULT_ID = "id";
    public static final String DEFAULT_TEXT = "text";
    public static final String DEFAULT_VECTOR = "vector";
    public static final String DEFAULT_METADATA = "metadata";
    public static final String DEFAULT_SCORE = "score";

    private final String idKey;
    private final String textKey;
    private final String vectorKey;
    private final String metadataKey;
    private final String scoreKey;

    private final VectorMappingConfig mapping;

    public VectorEmbeddingConfig(Map<String, Object> config, Map<String, Object> additionalHeaders, Map<String, Object> additionalBodies) {
        super(config, additionalHeaders, additionalBodies);
        this.vectorKey = (String) config.getOrDefault(EMBEDDING_KEY, DEFAULT_VECTOR);
        this.metadataKey = (String) config.getOrDefault(METADATA_KEY, DEFAULT_METADATA);
        this.scoreKey = (String) config.getOrDefault(SCORE_KEY, DEFAULT_SCORE);
        this.idKey = (String) config.getOrDefault(ID_KEY, DEFAULT_ID);
        this.textKey = (String) config.getOrDefault(TEXT_KEY, DEFAULT_TEXT);
        this.mapping = new VectorMappingConfig((Map<String, Object>) config.getOrDefault(MAPPING_KEY, Map.of()));
    }

    public String getIdKey() {
        return idKey;
    }

    public String getVectorKey() {
        return vectorKey;
    }

    public String getMetadataKey() {
        return metadataKey;
    }

    public String getScoreKey() {
        return scoreKey;
    }

    public String getTextKey() {
        return textKey;
    }

    public VectorMappingConfig getMapping() {
        return mapping;
    }
}
