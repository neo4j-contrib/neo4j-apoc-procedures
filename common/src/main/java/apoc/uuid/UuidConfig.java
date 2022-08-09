package apoc.uuid;

import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;

public class UuidConfig {

    private boolean addToExistingNodes;
    private boolean addToSetLabels;
    private String uuidProperty;

    private static final String DEFAULT_UUID_PROPERTY = "uuid";
    private static final boolean DEFAULT_ADD_TO_EXISTING_NODES = true;
    private static final boolean DEFAULT_ADD_TO_SET_LABELS = false;


    public UuidConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.addToExistingNodes = toBoolean(config.getOrDefault("addToExistingNodes", DEFAULT_ADD_TO_EXISTING_NODES));
        this.addToSetLabels = toBoolean(config.getOrDefault("addToSetLabels", DEFAULT_ADD_TO_SET_LABELS));
        this.uuidProperty = config.getOrDefault("uuidProperty", DEFAULT_UUID_PROPERTY).toString();

    }

    public UuidConfig() {} // for Jackson deserialization

    public boolean isAddToExistingNodes() {
        return addToExistingNodes;
    }

    public void setAddToExistingNodes(boolean addToExistingNodes) {
        this.addToExistingNodes = addToExistingNodes;
    }

    public String getUuidProperty() {
        return uuidProperty;
    }

    public void setUuidProperty(String uuidProperty) {
        this.uuidProperty = uuidProperty;
    }

    public boolean isAddToSetLabels() {
        return addToSetLabels;
    }
}
