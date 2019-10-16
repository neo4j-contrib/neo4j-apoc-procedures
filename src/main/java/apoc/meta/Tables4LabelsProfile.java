package apoc.meta;

import apoc.meta.tablesforlabels.PropertyContainerProfile;
import apoc.meta.tablesforlabels.OrderedLabels;
import apoc.meta.tablesforlabels.PropertyTracker;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

import java.util.*;
import java.util.stream.Stream;

public class Tables4LabelsProfile {
    Map<OrderedLabels, PropertyContainerProfile> labelMap;
    Map<String, PropertyContainerProfile> relMap;
    Map<OrderedLabels,Long> obsByNode;
    Map<String,Long> obsByRelType;

    /**
     * DAO class that the stored procedure returns
     */
    public class NodeTypePropertiesEntry {
        public String nodeType;
        public List<String> nodeLabels;
        public String propertyName;
        public List<String> propertyTypes;
        public boolean mandatory;
        public long propertyObservations;
        public long totalObservations;

        public NodeTypePropertiesEntry(String nodeType, List<String> nodeLabels, String propertyName, List<String>propertyTypes, boolean mandatory, long propertyObservations, long totalObservations) {
            this.nodeType = nodeType;
            this.nodeLabels = nodeLabels;
            this.propertyName = propertyName;
            this.propertyTypes = propertyTypes;
            this.mandatory = mandatory;
            this.propertyObservations = propertyObservations;
            this.totalObservations = totalObservations;
        }
    }

    public class RelTypePropertiesEntry {
        public String relType;
        public String propertyName;
        public List<String>propertyTypes;
        public boolean mandatory;
        public long propertyObservations;
        public long totalObservations;

        public RelTypePropertiesEntry(String relType, String propertyName, List<String>propertyTypes, boolean mandatory, long propertyObservations, long totalObservations) {
            this.relType = relType;
            this.propertyName = propertyName;
            this.propertyTypes = propertyTypes;
            this.mandatory = mandatory;
            this.propertyObservations = propertyObservations;
            this.totalObservations = totalObservations;
        }
    }

    public Tables4LabelsProfile() {
        labelMap = new LinkedHashMap(100);
        relMap = new LinkedHashMap(100);
        obsByNode = new LinkedHashMap(100);
        obsByRelType = new LinkedHashMap(100);
    }

    public void noteIndex(Label label, IndexDefinition id) {

    }

    public void noteConstraint(Label label, ConstraintDefinition cd) {

    }

    public PropertyContainerProfile getNodeProfile(OrderedLabels ol) {
        if (labelMap.containsKey(ol)) {
            return labelMap.get(ol);
        } else {
            PropertyContainerProfile p = new PropertyContainerProfile();
            labelMap.put(ol, p);
            return p;
        }
    }

    public PropertyContainerProfile getRelProfile(String relType) {
        if (relMap.containsKey(relType)) {
            return relMap.get(relType);
        } else {
            PropertyContainerProfile p = new PropertyContainerProfile();
            relMap.put(relType, p);
            return p;
        }
    }

    public Long sawNode(OrderedLabels labels) {
        if (obsByNode.containsKey(labels)) {
            Long val = obsByNode.get(labels) + 1;
            obsByNode.put(labels, val);
            return val;
        } else {
            obsByNode.put(labels, 1L);
            return 1L;
        }
    }

    public Long sawRel(String typeName) {
        if (obsByRelType.containsKey(typeName)) {
            Long val = obsByRelType.get(typeName) + 1;
            obsByRelType.put(typeName, val);
            return val;
        } else {
            obsByRelType.put(typeName, 1L);
            return 1L;
        }
    }

    public void observe(Node n, MetaConfig config) {
        OrderedLabels labels = new OrderedLabels(n.getLabels());
        PropertyContainerProfile localNodeProfile = getNodeProfile(labels);

        Set<String> excludes = config.getExcludes();
        Set<String> includesRels = config.getIncludesRels();

        sawNode(labels);
        localNodeProfile.observe(n);

        for (RelationshipType type : n.getRelationshipTypes()) {
            String typeName = type.name();
            if (excludes.contains(typeName)) {
                continue;
            }
            if (!includesRels.isEmpty() && !includesRels.contains(typeName)) {
                continue;
            }

            int out = n.getDegree(type, Direction.OUTGOING);
            if (out == 0) continue;

            long seenSoFar = sawRel(typeName);

            if (seenSoFar > config.getMaxRels()) {
                // We've seen more than the maximum sample size for this rel, so
                // we don't need to keep looking.
                continue;
            }

            PropertyContainerProfile localRelProfile = getRelProfile(typeName);

            for(Relationship r : n.getRelationships(type, Direction.OUTGOING)) {
                localRelProfile.observe(r);
            }
        }
    }

    public Tables4LabelsProfile finished() {
        for (PropertyContainerProfile prof : labelMap.values()) {
            prof.finished();
        }

        for(PropertyContainerProfile prof : relMap.values()) {
            prof.finished();
        }

        return this;
    }

    public Stream<NodeTypePropertiesEntry> asNodeStream() {
        Set<OrderedLabels> labels = labelMap.keySet();
        List<NodeTypePropertiesEntry> results = new ArrayList<NodeTypePropertiesEntry>(100);

        for(OrderedLabels ol : labels) {
            PropertyContainerProfile prof = labelMap.get(ol);
            Long totalObservations = obsByNode.get(ol);

            // Base case: the node never had any properties.
            if (prof.propertyNames().size() == 0) {
                results.add(new NodeTypePropertiesEntry(
                        ol.asNodeType(),
                        ol.nodeLabels(),
                        null,
                        null,
                        false, 0L,
                        totalObservations));
                continue;
            }

            for (String propertyName : prof.propertyNames()) {
                PropertyTracker tracker = prof.trackerFor(propertyName);

                NodeTypePropertiesEntry entry = new NodeTypePropertiesEntry(
                        ol.asNodeType(),
                        ol.nodeLabels(),
                        propertyName,
                        tracker.propertyTypes(),
                        tracker.mandatory,
                        tracker.observations,
                        totalObservations);

                results.add(entry);
            }
        }

        return results.stream();
    }

    public Stream<RelTypePropertiesEntry> asRelStream() {
        Set<String> relTypes = relMap.keySet();
        List<RelTypePropertiesEntry> results = new ArrayList<>(100);

        for (String relType : relTypes) {
            PropertyContainerProfile prof = relMap.get(relType);
            Long totalObservations = obsByRelType.get(relType);

            // Base case: the rel type never had any properties.
            if (prof.propertyNames().size() == 0) {
                results.add(new RelTypePropertiesEntry(
                        ":`" + relType + "`",
                        null,
                        null,
                        false,
                        0L,
                        totalObservations));
                continue;
            }

            for (String propertyName : prof.propertyNames()) {
                PropertyTracker tracker = prof.trackerFor(propertyName);

                results.add(new RelTypePropertiesEntry(
                        ":`" + relType + "`",
                        propertyName,
                        tracker.propertyTypes(),
                        tracker.mandatory,
                        tracker.observations,
                        totalObservations));
            }
        }

        return results.stream();
    }
}
