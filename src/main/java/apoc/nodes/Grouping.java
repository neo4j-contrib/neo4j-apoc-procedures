package apoc.nodes;

import apoc.Description;
import apoc.result.GraphResult;
import apoc.result.VirtualNode;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 14.06.17
 */
public class Grouping {

    @Context
    public GraphDatabaseService db;

    static class Key {
        private final int hash;
        private final String label;
        private final Map<String,Object> values;

        public Key(String label, Map<String, Object> values) {
            this.label = label;
            this.values = values;
            hash = 31 * label.hashCode() + values.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;
            return label.equals(key.label) && values.equals(key.values);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    @Procedure
    @Description("Group all nodes and their relationships by given keys, create virtual nodes and relationships for the summary information")
    public Stream<GraphResult> group(@Name("labels") List<String> labels, @Name("grouping") List<String> grouping) {
        String[] keys = grouping.toArray(new String[grouping.size()]);

        Map<Key,Set<Node>> grouped = new HashMap<>();
        Map<Key,Node> virtual = new HashMap<>();
        Map<RelKey,Relationship> virtualRels = new HashMap<>();

        for (String labelName : labels) {
            Label label = Label.label(labelName);
            Label[] singleLabel = {label};

            try (ResourceIterator<Node> nodes = db.findNodes(label)) {
                while (nodes.hasNext()) {
                    Node node = nodes.next();
                    Key key = keyFor(node, labelName, keys);
                    grouped.compute(key, (k, v) -> {if (v == null) v = new HashSet<>(); v.add(node); return v;});
                    virtual.compute(key, (k, v) -> {
                        if (v == null) {
                            v = new VirtualNode(singleLabel,k.values,db);
                        }
                        v.setProperty("count", ((Number)v.getProperty("count",0)).longValue() +1 );
                        return v;}
                    );
                }
            }
        }
        for (Map.Entry<Key, Set<Node>> entry : grouped.entrySet()) {
            for (Node node : entry.getValue()) {
                Key startKey = entry.getKey();
                Node v1 = virtual.get(startKey);
                for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
                    Node endNode = rel.getEndNode();
                    for (Key endKey : keysFor(endNode, labels, keys)) {
                        Node v2 = virtual.get(endKey);
                        if (v2 == null) continue;
                        virtualRels.compute(new RelKey(startKey,endKey,rel), (rk,vRel) -> {
                            if (vRel == null) vRel = v1.createRelationshipTo(v2, rel.getType());
                            vRel.setProperty("count", ((Number)vRel.getProperty("count",0)).longValue() +1 );
                            return vRel;
                        });
                    }
                }
            }
        }
        return virtual.values().stream().map( n -> new GraphResult(Collections.singletonList(n), Iterables.asList(n.getRelationships())));
    }

    public Key keyFor(Node node, String label, String[] keys) {
        Map<String, Object> props = node.getProperties(keys);
        return new Key(label, props);
    }

    public Collection<Key> keysFor(Node node, List<String> labels, String[] keys) {
        Map<String, Object> props = node.getProperties(keys);
        List<Key> result=new ArrayList<>(labels.size());
        for (Label label : node.getLabels()) {
            if (labels.contains(label.name())) {
                result.add(new Key(label.name(), props));
            }
        }
        return result;
    }

    private static class RelKey {
        private final int hash;
        private final Key startKey;
        private final Key endKey;
        private final String type;

        public RelKey(Key startKey, Key endKey, Relationship rel) {
            this.startKey = startKey;
            this.endKey = endKey;
            this.type = rel.getType().name();
            hash = 31 * (31 * startKey.hashCode() + endKey.hashCode()) + type.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RelKey relKey = (RelKey) o;

            return startKey.equals(relKey.startKey) && endKey.equals(relKey.endKey) && type.equals(relKey.type);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
