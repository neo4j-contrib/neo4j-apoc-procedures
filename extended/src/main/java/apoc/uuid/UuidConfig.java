package apoc.uuid;

import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;

public class UuidConfig {

    private boolean createConstraint = true;
    private boolean addToExistingNodes;
    private boolean addToSetLabels;
    private String uuidProperty;
    public static final String ADD_TO_SET_LABELS_KEY = "addToSetLabels";
    public static final String UUID_PROPERTY_KEY = "uuidProperty";
    public static final String ADD_TO_EXISTING_NODES_KEY = "addToExistingNodes";

    public static final String DEFAULT_UUID_PROPERTY = "uuid";
    private static final boolean DEFAULT_ADD_TO_EXISTING_NODES = true;
    public static final boolean DEFAULT_ADD_TO_SET_LABELS = false;


    public UuidConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.addToExistingNodes = toBoolean(config.getOrDefault(ADD_TO_EXISTING_NODES_KEY, DEFAULT_ADD_TO_EXISTING_NODES));
        this.addToSetLabels = toBoolean(config.getOrDefault(ADD_TO_SET_LABELS_KEY, DEFAULT_ADD_TO_SET_LABELS));
        this.uuidProperty = config.getOrDefault(UUID_PROPERTY_KEY, DEFAULT_UUID_PROPERTY).toString();

    }

    public boolean isAddToExistingNodes() {
        return addToExistingNodes;
    }

    public boolean isCreateConstraint() {
        return createConstraint;
    }

    public void setCreateConstraint(boolean createConstraint) {
        this.createConstraint = createConstraint;
    }

    public void setAddToExistingNodes(boolean addToExistingNodes) {
        this.addToExistingNodes = addToExistingNodes;
    }

    public String getUuidProperty() {
        return uuidProperty;
    }

    public boolean isAddToSetLabels() {
        return addToSetLabels;
    }
}

