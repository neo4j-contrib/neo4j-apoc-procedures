package apoc.meta;

import apoc.meta.tablesforlabels.PropertyContainerProfile;
import apoc.meta.tablesforlabels.OrderedLabels;
import apoc.meta.tablesforlabels.PropertyTracker;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.stream.Collectors;

public class Tables4LabelsProfile {
    Map<OrderedLabels, PropertyContainerProfile> labelMap;
    Map<String, PropertyContainerProfile> relMap;
    Map<OrderedLabels,Long> obsByNode;
    Map<String,Long> obsByRelType;
    Map<String,Map<String,List<String>>> relGlobalMeta;

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
        public List<String> sourceNodeLabels;
        public List<String> targetNodeLabels;
        public String propertyName;
        public List<String>propertyTypes;
        public boolean mandatory;
        public long propertyObservations;
        public long totalObservations;

        public RelTypePropertiesEntry(String relType, List<String> sourceNodeLabels, List<String> targetNodeLabels, String propertyName, List<String>propertyTypes, boolean mandatory, long propertyObservations, long totalObservations) {
            this.relType = relType;
            this.sourceNodeLabels = sourceNodeLabels;
            this.targetNodeLabels = targetNodeLabels;
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
        relGlobalMeta = new LinkedHashMap(100);
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

    public static String labelJoin(Iterable<Label> labels) {
        return StreamSupport.stream(labels.spliterator(), true)
        .map(Label::name)
        .collect(Collectors.joining("@@@"));
    }

    public static class decipherRelMap {
        public static List<String> getSourceLabels(String relMapIdentifier) {
            String[] components = relMapIdentifier.split("###");
            List<String> sourceNodeLabels = Arrays.asList(components[0].split("@@@"));
            return sourceNodeLabels;
        }
        public static List<String> getTargetLabels(String relMapIdentifier) {
            String[] components = relMapIdentifier.split("###");
            List<String> targetNodeLabels = Arrays.asList(components[1].split("@@@"));
            return targetNodeLabels;
        }
        public static String getRelType(String relMapIdentifier) {
            String[] components = relMapIdentifier.split("###");
            String relTypeName = components[2];
            return relTypeName;
        }
    }

    public void observe(Node n, MetaConfig config) {
        OrderedLabels labels = new OrderedLabels(n.getLabels());
        PropertyContainerProfile localNodeProfile = getNodeProfile(labels);

        // Only descend and look at properties if it's in our match list.
        if (config.matches(n.getLabels())) {
            sawNode(labels);
            localNodeProfile.observe(n, true);
        }

        // Even if the node isn't in our match list, do rel processing.  This
        // is because our profiling is "node-first" to get to the relationships,
        // and if we don't do it this way, it's possible to blacklist nodes and
        // thereby miss relationships that were of interest.
        for (RelationshipType type : n.getRelationshipTypes()) {
            String typeName = type.name();

            if (!config.matches(type)) { continue; }

            int out = n.getDegree(type, Direction.OUTGOING);
            if (out == 0) continue;

            for(Relationship r : n.getRelationships(Direction.OUTGOING, type)) {
                Iterable relStartNode = r.getStartNode().getLabels();
                Iterable relEndNode = r.getEndNode().getLabels();

                String relIdentifier = labelJoin(relStartNode) + "###" + labelJoin(relEndNode) + "###" + typeName;

                PropertyContainerProfile localRelProfile = getRelProfile(relIdentifier);
                long seenSoFar = sawRel(relIdentifier);
                boolean isNode = false;

                if (seenSoFar > config.getMaxRels()) {
                    // We've seen more than the maximum sample size for this rel, so
                    // we don't need to keep looking.
                    continue;
                }

                localRelProfile.observe(r, isNode);
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
        List<NodeTypePropertiesEntry> results = new ArrayList<>( 100 );

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
                        ":`" + decipherRelMap.getRelType(relType) + "`",
                        decipherRelMap.getSourceLabels(relType),
                        decipherRelMap.getTargetLabels(relType),
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
                        ":`" + decipherRelMap.getRelType(relType) + "`",
                        decipherRelMap.getSourceLabels(relType),
                        decipherRelMap.getTargetLabels(relType),
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
