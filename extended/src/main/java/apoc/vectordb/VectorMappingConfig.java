package apoc.vectordb;

import apoc.util.Util;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

public class VectorMappingConfig {
    public static final String ID_KEY = "id";
    public static final String PROP_KEY = "prop";
    public static final String LABEL_KEY = "label";
    public static final String TYPE_KEY = "type";
    public static final String EMBEDDING_PROP_KEY = "embeddingProp";
    public static final String SIMILARITY_KEY = "similarity";
    public static final String CREATE_KEY = "create";

    private final String id;
    private final String prop;

    private final String label;
    private final String type;
    private final String embeddingProp;
    private final String similarity;

    private final boolean create;

    public VectorMappingConfig(Map<String, Object> mapping) {
        if (mapping == null) {
            mapping = getMappingConf();
        }
        this.id = (String) mapping.get(ID_KEY);
        this.prop = (String) mapping.get(PROP_KEY);

        this.label = (String) mapping.get(LABEL_KEY);
        this.type = (String) mapping.get(TYPE_KEY);
        this.embeddingProp = (String) mapping.get(EMBEDDING_PROP_KEY);

        this.similarity = (String) mapping.getOrDefault(SIMILARITY_KEY, "cosine");

        this.create = Util.toBoolean(mapping.get(CREATE_KEY));
    }

    @NotNull
    private static Map<String, Object> getMappingConf() {
        Map<String, Object> mapping;
        mapping = Collections.emptyMap();
        return mapping;
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
