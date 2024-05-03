package apoc.vectordb;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class VectorMappingConfig {
    private final String id;
    private final String prop;

    private final String label;
    private final String type;
    private final String embeddingProp;
    private final String similarity;

    private final boolean create;

    public VectorMappingConfig(Map<String, Object> mapping) {
        if (mapping == null) {
            mapping = Collections.emptyMap();
        }
        this.id = (String) mapping.get("id");
        this.prop = (String) mapping.get("prop");

        this.label = (String) mapping.get("label");
        this.type = (String) mapping.get("type");
        this.embeddingProp = (String) mapping.get("embeddingProp");

        this.similarity = (String) mapping.getOrDefault("similarity", "cosine");

        this.create = Util.toBoolean(mapping.get("create"));
    }

    public String getId() {
        return id;
    }

    public String getProp() {
        return prop;
    }

    public String getLabel() {
        return label;
    }

    public String getType() {
        return type;
    }

    public String getEmbeddingProp() {
        return embeddingProp;
    }

    public boolean isCreate() {
        return create;
    }

    public String getSimilarity() {
        return similarity;
    }
}
