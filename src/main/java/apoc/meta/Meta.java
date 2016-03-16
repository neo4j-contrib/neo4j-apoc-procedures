package apoc.meta;

import apoc.result.*;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.min;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class Meta {

    private static final Label[] META = new Label[] {Label.label("Meta")};

    @Context
    public GraphDatabaseService db;

    @Procedure
    public Stream<GraphResult> graph() {
        Map<String,Node> labels = new TreeMap<>();
        Map<List<String>,Relationship> rels = new HashMap<>();
        for (Relationship rel : db.getAllRelationships()) {
            addLabels(labels,rel.getStartNode());
            addLabels(labels,rel.getEndNode());
            addRel(rels,labels, rel);
        }
        return Stream.of(new GraphResult(new ArrayList<>(labels.values()), new ArrayList<>(rels.values())));
    }

    private void addRel(Map<List<String>, Relationship> rels, Map<String, Node> labels, Relationship rel) {
        String typeName = rel.getType().name();
        Node startNode = rel.getStartNode();
        Node endNode = rel.getEndNode();
        for (Label labelA : startNode.getLabels()) {
            Node nodeA = labels.get(labelA.name());
            for (Label labelB : endNode.getLabels()) {
                List<String> key = asList(labelA.name(), labelB.name(), typeName);
                Relationship vRel = rels.get(key);
                if (vRel==null) {
                    Node nodeB = labels.get(labelB.name());
                    vRel = new VirtualRelationship(nodeA,nodeB,rel.getType()).withProperties(singletonMap("type",typeName));
                    rels.put(key,vRel);
                }
                vRel.setProperty("count",((int)vRel.getProperty("count",0))+1);
            }
        }
    }

    private void addLabels(Map<String, Node> labels, Node node) {
        for (Label label : node.getLabels()) {
            String name = label.name();
            Node vNode = labels.get(name);
            if (vNode == null) {
                vNode = new VirtualNode(META, Collections.singletonMap("name", name),db);
                labels.put(name, vNode);
            }
            vNode.setProperty("count",((int)vNode.getProperty("count",0))+1);
        }
    }

    static class RelInfo {
        final Set<String> properties = new HashSet<>();
        final NodeInfo from,to;
        final String type;
        int count;

        public RelInfo(NodeInfo from, NodeInfo to, String type) {
            this.from = from;
            this.to = to;
            this.type = type;
        }
        public void add(Relationship relationship) {
            for (String key : relationship.getPropertyKeys()) properties.add(key);
            count++;
        }
    }
    static class NodeInfo {
        final Set<String> labels=new HashSet<>();
        final Set<String> properties = new HashSet<>();
        long count, minDegree, maxDegree, sumDegree;

        private void add(Node node) {
            count++;
            int degree = node.getDegree();
            sumDegree += degree;
            if (degree > maxDegree) maxDegree = degree;
            if (degree < minDegree) minDegree = degree;

            for (Label label : node.getLabels()) labels.add(label.name());
            for (String key : node.getPropertyKeys()) properties.add(key);
        }
        Map<String,Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("labels",labels.toArray());
            map.put("properties",properties.toArray());
            map.put("count",count);
            map.put("minDegree",minDegree);
            map.put("maxDegree",maxDegree);
            map.put("avgDegree",sumDegree/count);
            return map;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof NodeInfo && labels.equals(((NodeInfo) o).labels);

        }

        @Override
        public int hashCode() {
            return labels.hashCode();
        }
    }
}
