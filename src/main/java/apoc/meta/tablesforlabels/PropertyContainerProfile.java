package apoc.meta.tablesforlabels;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;

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

    public void observe(PropertyContainer n) {
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
        }
    }

    public PropertyContainerProfile finished() {
        for(PropertyTracker tracker: profile.values()) {
            tracker.finished(observations);
        }
        return this;
    }
}
