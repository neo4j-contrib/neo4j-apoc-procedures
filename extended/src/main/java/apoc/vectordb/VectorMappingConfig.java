package apoc.vectordb;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class VectorMappingConfig {
    public static final String METADATA_KEY = "metadataKey";
    public static final String ENTITY_KEY = "entityKey";
    public static final String NODE_LABEL = "nodeLabel";
    public static final String REL_TYPE = "relType";
    public static final String EMBEDDING_KEY = "embeddingKey";
    public static final String SIMILARITY_KEY = "similarity";
    public static final String CREATE_KEY = "create";

    private final String metadataKey;
    private final String entityKey;

    private final String nodeLabel;
    private final String relType;
    private final String embeddingKey;
    private final String similarity;

    private final boolean create;
    private boolean updateMode = false;

    public VectorMappingConfig(Map<String, Object> mapping) {
        if (mapping == null) {
            mapping = Collections.emptyMap();
        }
        this.metadataKey = (String) mapping.get(METADATA_KEY);
        this.entityKey = (String) mapping.get(ENTITY_KEY);

        this.nodeLabel = (String) mapping.get(NODE_LABEL);
        this.relType = (String) mapping.get(REL_TYPE);
        this.embeddingKey = (String) mapping.get(EMBEDDING_KEY);

        this.similarity = (String) mapping.getOrDefault(SIMILARITY_KEY, "cosine");

        this.create = Util.toBoolean(mapping.get(CREATE_KEY));
    }

    public String getMetadataKey() {
        return metadataKey;
    }

    public String getEntityKey() {
        return entityKey;
    }

    public String getNodeLabel() {
        return nodeLabel;
    }

    public String getRelType() {
        return relType;
    }

    public String getEmbeddingKey() {
        return embeddingKey;
    }

    public boolean isCreate() {
        return create;
    }

    public String getSimilarity() {
        return similarity;
    }

    public boolean isUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(boolean updateMode) {
        this.updateMode = updateMode;
    }
}
