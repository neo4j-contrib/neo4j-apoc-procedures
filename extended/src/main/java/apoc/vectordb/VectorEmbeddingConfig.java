package apoc.vectordb;

import apoc.ml.RestAPIConfig;
import apoc.util.Util;

import java.util.Map;

public class VectorEmbeddingConfig {
    public static final String FIELDS_KEY = "fields";
    public static final String VECTOR_KEY = "vectorKey";
    public static final String METADATA_KEY = "metadataKey";
    public static final String SCORE_KEY = "scoreKey";
    public static final String TEXT_KEY = "textKey";
    public static final String ID_KEY = "idKey";
    public static final String MAPPING_KEY = "mapping";
    
    public static final String DEFAULT_ID = "id";
    public static final String DEFAULT_TEXT = "text";
    public static final String DEFAULT_ERRORS = "errors";
    public static final String DEFAULT_VECTOR = "vector";
    public static final String DEFAULT_METADATA = "metadata";
    public static final String DEFAULT_SCORE = "score";
    public static final String ALL_RESULTS_KEY = "allResults";
    public static final String META_AS_SUBKEY_KEY = "metaAsSubKey";

    private final String idKey;
    private final String textKey;
    private final String vectorKey;
    private final String metadataKey;
    private final String scoreKey;
    private final String errorsKey;

    private final boolean allResults;
    private final boolean metaAsSubKey;

    private final VectorMappingConfig mapping;
    private final RestAPIConfig apiConfig;

    public VectorEmbeddingConfig(Map<String, Object> config) {
        this.vectorKey = (String) config.getOrDefault(VECTOR_KEY, DEFAULT_VECTOR);
        this.metadataKey = (String) config.getOrDefault(METADATA_KEY, DEFAULT_METADATA);
        this.scoreKey = (String) config.getOrDefault(SCORE_KEY, DEFAULT_SCORE);
        this.idKey = (String) config.getOrDefault(ID_KEY, DEFAULT_ID);
        this.textKey = (String) config.getOrDefault(TEXT_KEY, DEFAULT_TEXT);
        this.errorsKey = (String) config.getOrDefault(TEXT_KEY, DEFAULT_ERRORS);
        this.allResults = Util.toBoolean(config.get(ALL_RESULTS_KEY));
        this.mapping = new VectorMappingConfig((Map<String, Object>) config.getOrDefault(MAPPING_KEY, Map.of()));

        this.metaAsSubKey = Util.toBoolean(config.getOrDefault(META_AS_SUBKEY_KEY, true));
        
        this.apiConfig = new RestAPIConfig(config);
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

    public String getErrorsKey() {
        return errorsKey;
    }

    public boolean isAllResults() {
        return allResults;
    }

    public boolean isMetaAsSubKey() {
        return metaAsSubKey;
    }

    public VectorMappingConfig getMapping() {
        return mapping;
    }

    public RestAPIConfig getApiConfig() {
        return apiConfig;
    }
}
