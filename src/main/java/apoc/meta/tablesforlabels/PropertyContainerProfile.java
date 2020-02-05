package apoc.meta.tablesforlabels;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
//import org.neo4j.graphdb.PropertyContainer;
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
    Map<String, PropertyTracker> profile;

    public PropertyContainerProfile() {
        observations = 0;
        profile = new HashMap<>(3);
    }

    public Set<String> propertyNames() { return profile.keySet(); }
    public PropertyTracker trackerFor(String propName) { return profile.get(propName); }

    public void observe(Entity n, Iterable<ConstraintDefinition> constraints, boolean isNode, Map<String, Iterable<ConstraintDefinition>> relConstraints) {
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

            if (isNode) {

                // Check for node constraints

                tracker.mandatory = false;
                for (ConstraintDefinition cd : constraints) {
                    for (String pk : cd.getPropertyKeys()) {
                        if (pk == propName) {
                            tracker.mandatory = true;
                        }
                    }
                }
            } else {

                // Check for relationship constraints - NOTE: Could probably improve the efficiency here a bit. Too many nested loops.

                tracker.mandatory = false;
                for (Map.Entry<String,Iterable<ConstraintDefinition>> entry : relConstraints.entrySet()) {
                    for (ConstraintDefinition cd : entry.getValue()) {
                        for (String pk : cd.getPropertyKeys()) {
                            if (pk == propName) {
                                tracker.mandatory = true;
                            }
                        }
                    }
                }
            }
        }
    }

    public PropertyContainerProfile finished() {
        return this;
    }
}
