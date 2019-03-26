package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import apoc.result.VirtualGraph;
import apoc.result.VirtualNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DocumentToGraph {

    private GraphDatabaseService db;
    private RelationshipBuilder documentRelationBuilder;
    private LabelBuilder documentLabelBuilder;
    private GraphsConfig config;

    public DocumentToGraph(GraphDatabaseService db, GraphsConfig config) {
        this.db = db;
        this.documentRelationBuilder = new RelationshipBuilder(config);
        this.documentLabelBuilder = new LabelBuilder(config);
        this.config = config;
    }

    public void validate(Map map) {
        if (!map.containsKey(config.getIdField())) {
            throw new RuntimeException("every object must have " + config.getIdField() + " as `id` field name");
        } else if (!map.containsKey(config.getLabelField())) {
            throw new RuntimeException("every object must have " + config.getLabelField() + " as `label` field name");
        }
    }

    private void fromDocument(Map<String, Object> document, Node source, String type,
                              Map<String, Set<Node>> nodes, Set<Relationship> relationships) {
        boolean isRootNode = source == null;
        Label label = this.documentLabelBuilder.buildLabel(document);

        validate(document);

        Object idValue = document.get(config.getIdField());
        // retrieve the current node
        final Node node;
        if (this.config.isWrite()) {
            node = getOrCreateRealNode(label, idValue);
        } else {
            node = getOrCreateVirtualNode(nodes, label, idValue);
        }

        // write node properties
        document.entrySet().stream()
                .filter(e -> isSimpleType(e))
                .forEach(e -> {
                    Object value = e.getValue();
                    if (value instanceof List) {
                        List list = (List) value;
                        if (!list.isEmpty()) {
                            value = Array.newInstance(list.get(0).getClass(), list.size());
                            for (int i = 0; i < list.size(); i++) {
                                Array.set(value, i, list.get(i));
                            }
                        }
                    }
                    node.setProperty(e.getKey(), value);
                });

        // get child nodes
        document.entrySet().stream()
                .filter(e -> !isSimpleType(e))
                .forEach(e -> {
                    if (e.getValue() instanceof Map) { // if value is a complex object (map)
                        Map inner = (Map) e.getValue();
                        fromDocument(inner, node, e.getKey(), nodes, relationships);
                    } else {
                        List<Map> list = (List) e.getValue(); // if value is and array
                        list.forEach(map -> fromDocument(map, node, e.getKey(), nodes, relationships));
                    }
                });

        Set<Node> nodesWithSameIds = getNodesWithSameIds(nodes, idValue);
        nodesWithSameIds.add(node);
        if (!isRootNode) {
            relationships.addAll(documentRelationBuilder.buildRelation(source, node, type));
        }

    }

    private Set<Node> getNodesWithSameIds(Map<String, Set<Node>> nodes, Object idValue) {
        return nodes.computeIfAbsent(idValue.toString(), (k) -> new LinkedHashSet<>());
    }

    private Node getOrCreateVirtualNode(Map<String, Set<Node>> nodes, Label label, Object idValue) {
        Set<Node> nodesWithSameIds = getNodesWithSameIds(nodes, idValue);
        return nodesWithSameIds
                .stream()
                .filter(n -> {
                    if (n.hasLabel(label)) {
                        return true;
                    }
                    return StreamSupport.stream(n.getRelationships().spliterator(), false)
                            .anyMatch(r -> r.getOtherNode(n).hasLabel(label));
                })
                .findFirst()
                .orElse(new VirtualNode(new Label[]{label}, Collections.emptyMap(), db));
    }

    private Node getOrCreateRealNode(Label label, Object idValue) {
        Node nodeInDB = db.findNode(label, config.getIdField(), idValue);
        return nodeInDB != null ? nodeInDB : db.createNode(label);
    }

    private boolean isSimpleType(Map.Entry<String, Object> e) {
        if (e.getValue() instanceof Map) {
            return false;
        }
        if (e.getValue() instanceof List) {
            List list = (List) e.getValue();
            if (!list.isEmpty()) {
                Object object = list.get(0); // assumption: homogeneous array
                if (object instanceof Map) { // if is an array of complex type
                    return false;
                }
            }
        }
        return true;
    }

    public VirtualGraph create(Collection<Map> coll) {
        Map<String, Set<Node>> nodes = new HashMap<>();
        Set<Relationship> relationships = new LinkedHashSet<>();
        coll.forEach(map -> fromDocument(map, null, null, nodes, relationships));
        return new VirtualGraph("Graph", nodes.values().stream().flatMap(Set::stream).collect(Collectors.toCollection(LinkedHashSet::new)), relationships, Collections.emptyMap());
    }

}