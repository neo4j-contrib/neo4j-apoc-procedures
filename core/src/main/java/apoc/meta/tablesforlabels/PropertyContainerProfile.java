package apoc.meta.tablesforlabels;

import apoc.meta.Meta.ConstraintTracker;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.schema.ConstraintDefinition;

import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A profile of a particular ordered label set (or relationship type) has a set of possible properties that can exist, and
 * stats about those properties.
 */
public class PropertyContainerProfile {
    public long observations;
    public boolean isNode;
    Map<String, PropertyTracker> profile;

    public PropertyContainerProfile() {
        observations = 0;
        profile = new HashMap<>(3);
        isNode = false;
    }

    public Set<String> propertyNames() { return profile.keySet(); }
    public PropertyTracker trackerFor(String propName) { return profile.get(propName); }

    public void observe(Entity n, boolean isNode) {
        observations++;

        for (String propName : n.getPropertyKeys()) {
            PropertyTracker tracker;

            if (profile.containsKey(propName)) {
                tracker = profile.get(propName);
            } else {
                tracker = new PropertyTracker(propName);
                profile.put(propName, tracker);
            }

            tracker.addObservation(n.getProperty(propName));

            tracker.mandatory = false;
            this.isNode = isNode;
        }
    }

    public PropertyContainerProfile finished() {
        PropertyTracker tracker;

        for (String propName : this.propertyNames()) {
            if (this.isNode) {

                // Check for node constraints

                for (Map.Entry<String,List<String>> entry : ConstraintTracker.nodeConstraints.entrySet()) {
                    for (String pk : entry.getValue()) {
                        if (this.profile.containsKey(pk)) {
                            tracker = this.profile.get(pk);
                            tracker.mandatory = true;
                        }
                    }
                }
            } else {

                // Check for relationship constraints

                for (Map.Entry<String,List<String>> entry : ConstraintTracker.relConstraints.entrySet()) {
                    for (String pk : entry.getValue()) {
                        if (this.profile.containsKey(pk)) {
                            tracker = this.profile.get(pk);
                            tracker.mandatory = true;
                        }
                    }
                }
            }
        }

        return this;
    }
}
