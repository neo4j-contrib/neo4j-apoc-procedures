package apoc.graph.util;

import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;

public class GraphsConfig {

    private boolean write;
    private String labelField;
    private String idField;
    private boolean generateId;
    private String defaultLabel;

    public GraphsConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        write = toBoolean(config.getOrDefault("write", false));
        generateId = toBoolean(config.getOrDefault("generateId", true));
        idField = config.getOrDefault("idField", "id").toString();
        labelField = config.getOrDefault("labelField", "type").toString();
        defaultLabel = config.getOrDefault("defaultLabel", "DocNode").toString();
    }

    public boolean isWrite() {
        return write;
    }

    public String getLabelField() {
        return labelField;
    }

    public String getIdField() {
        return idField;
    }

    public boolean isGenerateId() {
        return generateId;
    }

    public String getDefaultLabel() {
        return defaultLabel;
    }
}
