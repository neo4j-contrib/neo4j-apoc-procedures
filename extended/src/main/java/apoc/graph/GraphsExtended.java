package apoc.graph;

import apoc.Extended;
import apoc.result.GraphResultExtended;
import apoc.result.VirtualNodeExtended;
import apoc.result.VirtualRelationshipExtended;
import apoc.util.collection.IterablesExtended;
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
    public Stream<GraphResultExtended> fromData(
            @Name("value") Object value,
            @Name(value = "nodePropertiesToRemove", defaultValue = "{}") Map<String, List<String>> nodePropertiesToRemove,
            @Name(value = "relPropertiesToRemove", defaultValue = "{}") Map<String, List<String>> relPropertiesToRemove) {
        
        VirtualGraphExtractor extractor = new VirtualGraphExtractor(nodePropertiesToRemove, relPropertiesToRemove);
        extractor.extract(value);
        GraphResultExtended result = new GraphResultExtended( extractor.nodes(), extractor.rels() );
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
        
        private final Map<String, Node> nodes;
        private final Map<String, Relationship> rels;
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
            if (value instanceof Node node) {
                addVirtualNode(node);
                
            } else if (value instanceof Relationship rel) {
                addVirtualRel(rel);
                
            } else if (value instanceof Path path) {
                path.nodes().forEach(this::addVirtualNode);
                path.relationships().forEach(this::addVirtualRel);
                
            } else if (value instanceof Iterable) {
                ((Iterable<?>) value).forEach(this::extract);
                
            } else if (value instanceof Map<?,?> map) {
                map.values().forEach(this::extract);
                
            } else if (value instanceof Iterator) {
                ((Iterator<?>) value).forEachRemaining(this::extract);
                
            } else if (value instanceof Object[] array) {
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
            rels.putIfAbsent(rel.getElementId(), createVirtualRel(rel));
        }

        private void addVirtualNode(Node node) {
            nodes.putIfAbsent(node.getElementId(), createVirtualNode(node));
        }

        private Node createVirtualNode(Node startNode) {
            List<String> props = IterablesExtended.asList(startNode.getPropertyKeys());
            nodePropertiesToRemove.forEach((k,v) -> {
                if (k.equals(ALL_FILTER) || startNode.hasLabel(Label.label(k))) {
                    props.removeAll(v);
                }
            });

            return new VirtualNodeExtended(startNode, props);
        }

        private Relationship createVirtualRel(Relationship rel) {
            Node startNode = rel.getStartNode();
            startNode = nodes.putIfAbsent(startNode.getElementId(), createVirtualNode(startNode));

            Node endNode = rel.getEndNode();
            endNode = nodes.putIfAbsent(endNode.getElementId(), createVirtualNode(endNode));
            
            Map<String, Object> props = rel.getAllProperties();
            
            relPropertiesToRemove.forEach((k,v) -> {
                if (k.equals(ALL_FILTER) || rel.isType(RelationshipType.withName(k))) {
                    v.forEach(props.keySet()::remove);
                }
            });

            return new VirtualRelationshipExtended(startNode, endNode, rel.getType(), props);
        }

        public List<Node> nodes() {
            return List.copyOf(nodes.values());
        }

        public List<Relationship> rels() {
            return List.copyOf(rels.values());
        }
    }
}
