package apoc.uuid;

import apoc.ExtendedSystemPropertyKeys;
import org.neo4j.graphdb.Node;

import java.util.Collections;
import java.util.Map;

import static apoc.uuid.UuidConfig.ADD_TO_SET_LABELS_KEY;
import static apoc.uuid.UuidConfig.UUID_PROPERTY_KEY;

public class UuidInfo {
    public final String label;
    public final boolean installed;
    public final Map<String, Object> properties;

    UuidInfo(String label, boolean installed, Map<String, Object> properties) {
        this.label = label;
        this.installed = installed;
        this.properties = properties;
    }

    UuidInfo(boolean installed) {
        this(null, installed, Collections.emptyMap());
    }

    public UuidInfo(Node node, boolean installed) {
        this.label = (String) node.getProperty(ExtendedSystemPropertyKeys.label.name());
        boolean addToSetLabel = (boolean) node.getProperty(ExtendedSystemPropertyKeys.addToSetLabel.name());
        String propertyName = (String) node.getProperty(ExtendedSystemPropertyKeys.propertyName.name());
        this.properties = Map.of(UUID_PROPERTY_KEY, propertyName,
                ADD_TO_SET_LABELS_KEY, addToSetLabel);
        this.installed = installed;
    }

    public UuidInfo(Node node) {
        this(node, false);
    }
}