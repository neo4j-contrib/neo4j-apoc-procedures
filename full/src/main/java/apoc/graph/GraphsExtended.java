package apoc.graph;

import apoc.Extended;
import apoc.result.GraphResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Extended
public class GraphsExtended {

    @Procedure("apoc.graph.filterProperties")
    @Description(
            "CALL apoc.graph.filterProperties(anyEntityObject, nodePropertiesToRemove, relPropertiesToRemove) YIELD nodes, relationships - returns a set of virtual nodes and relationships without the properties defined in nodePropertiesToRemove and relPropertiesToRemove")
    public Stream<GraphResult> fromData(
            @Name("value") Object value,
            @Name(value = "nodePropertiesToRemove", defaultValue = "{}") Map<String, List<String>> nodePropertiesToRemove,
            @Name(value = "relPropertiesToRemove", defaultValue = "{}") Map<String, List<String>> relPropertiesToRemove) {
        
        VirtualGraphExtractor extractor = new VirtualGraphExtractor(nodePropertiesToRemove, relPropertiesToRemove);
        extractor.extract(value);
        GraphResult result = new GraphResult( extractor.nodes(), extractor.rels() );
        return Stream.of(result);
    }
    
    @UserAggregationFunction("apoc.graph.filterProperties")
    @Description(
            "apoc.graph.filterProperties(anyEntityObject, nodePropertiesToRemove, relPropertiesToRemove) - aggregation function which returns an object {node: [virtual nodes], relationships: [virtual relationships]} without the properties defined in nodePropertiesToRemove and relPropertiesToRemove")
    public GraphFunction filterProperties() {
        return new GraphFunction();
    }

    public static class GraphFunction {
        public static final String NODES = "nodes";
        public static final String RELATIONSHIPS = "relationships";

        private VirtualGraphExtractor virtualGraphExtractor;

        @UserAggregationUpdate
        public void filterProperties(
                @Name("value") Object value,
                @Name(value = "nodePropertiesToRemove", defaultValue = "{}") Map<String, List<String>> nodePropertiesToRemove,
                @Name(value = "relPropertiesToRemove", defaultValue = "{}") Map<String, List<String>> relPropertiesToRemove) {
            
            if (virtualGraphExtractor == null) {
                virtualGraphExtractor = new VirtualGraphExtractor(nodePropertiesToRemove, relPropertiesToRemove);
            }
            virtualGraphExtractor.extract(value);
        }

        @UserAggregationResult
        public Object result() {
            Collection<Node> nodes = virtualGraphExtractor.nodes();
            Collection<Relationship> relationships = virtualGraphExtractor.rels();
            return Map.of(
                    NODES, nodes,
                    RELATIONSHIPS, relationships
            );
        }
    }

    public static class VirtualGraphExtractor {
        private static final String ALL_FILTER = "_all";
        
        private final Map<Long, Node> nodes;
        private final Map<Long, Relationship> rels;
        private final Map<String, List<String>> nodePropertiesToRemove;
        private final Map<String, List<String>> relPropertiesToRemove;

        public VirtualGraphExtractor(Map<String, List<String>> nodePropertiesToRemove, Map<String, List<String>> relPropertiesToRemove) {
            this.nodes = new HashMap<>();
            this.rels = new HashMap<>();
            this.nodePropertiesToRemove = nodePropertiesToRemove;
            this.relPropertiesToRemove = relPropertiesToRemove;
        }

        public void extract(Object value) {
            if (value == null) {
                return;
            }
            if (value instanceof Node) {
                Node node = (Node) value;
                addVirtualNode(node);
                
            } else if (value instanceof Relationship) {
                Relationship rel = (Relationship) value;
                addVirtualRel(rel);
                
            } else if (value instanceof Path) {
                Path path = (Path) value;
                path.nodes().forEach(this::addVirtualNode);
                path.relationships().forEach(this::addVirtualRel);
                
            } else if (value instanceof Iterable) {
                ((Iterable<?>) value).forEach(this::extract);
                
            } else if (value instanceof Map<?, ?>) {
                Map<?, ?> map = (Map<?, ?>) value;
                map.values().forEach(this::extract);
                
            } else if (value instanceof Iterator) {
                ((Iterator<?>) value).forEachRemaining(this::extract);
                
            } else if (value instanceof Object[]) {
                Object[] array = (Object[]) value;
                for (Object i : array) {
                    extract(i);
                }
            }
        }

        /**
         * We can use the elementId as a unique key for virtual nodes/relations, 
         * as it is the same as the analogue for real nodes/relations.
         */
        private void addVirtualRel(Relationship rel) {
            rels.putIfAbsent(rel.getId(), createVirtualRel(rel));
        }

        private void addVirtualNode(Node node) {
            nodes.putIfAbsent(node.getId(), createVirtualNode(node));
        }

        private Node createVirtualNode(Node startNode) {
            List<String> props = Iterables.asList(startNode.getPropertyKeys());
            nodePropertiesToRemove.forEach((k,v) -> {
                if (k.equals(ALL_FILTER) || startNode.hasLabel(Label.label(k))) {
                    props.removeAll(v);
                }
            });

            return new VirtualNode(startNode, props);
        }

        private Relationship createVirtualRel(Relationship rel) {
            Node startNode = rel.getStartNode();
            startNode = nodes.putIfAbsent(startNode.getId(), createVirtualNode(startNode));

            Node endNode = rel.getEndNode();
            endNode = nodes.putIfAbsent(endNode.getId(), createVirtualNode(endNode));
            
            Map<String, Object> props = rel.getAllProperties();
            
            relPropertiesToRemove.forEach((k,v) -> {
                if (k.equals(ALL_FILTER) || rel.isType(RelationshipType.withName(k))) {
                    v.forEach(props.keySet()::remove);
                }
            });

            return new VirtualRelationship(startNode, endNode, rel.getType(), props);
        }

        public List<Node> nodes() {
            return List.copyOf(nodes.values());
        }

        public List<Relationship> rels() {
            return List.copyOf(rels.values());
        }
    }
}
