package apoc.agg;

import apoc.coll.SetBackedList;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static apoc.util.Util.map;

/**
 * @author mh
 * @since 18.12.17
 */
public class Graph {
    @UserAggregationFunction("apoc.agg.graph")
    @Description("apoc.agg.graph(path) - returns map of graph {nodes, relationships} of all distinct nodes and relationships")
    public GraphAggregation graph() {
        return new GraphAggregation();
    }


    public static class GraphAggregation {

        private Set<Node> nodes = new HashSet<>();
        private Set<Relationship> rels = new HashSet<>();
        private Set<Relationship> plainRels = new HashSet<>();

        @UserAggregationUpdate
        public void aggregate(@Name("element") Object element) {
            consume(element);
        }

        public void consume(@Name("element") Object element) {
            if (element instanceof Node) {
                nodes.add((Node)element);
            }
            if (element instanceof Relationship) {
                plainRels.add((Relationship) element);
            }
            if (element instanceof Path) {
                Path path = (Path) element;
                for (Node node : path.nodes()) nodes.add(node);
                for (Relationship rel : path.relationships()) rels.add(rel);
            }
            if (element instanceof Map) {
                ((Map)element).values().forEach(this::consume);
            }
            if (element instanceof Iterable) {
                ((Iterable)element).forEach(this::consume);
            }
        }

        @UserAggregationResult
        public Map<String,Object> result() {
            if (!plainRels.isEmpty()) {
                for (Relationship rel : plainRels) {
                    nodes.add(rel.getStartNode());
                    nodes.add(rel.getEndNode());
                }
                rels.addAll(plainRels);
            }
            return map("nodes", new SetBackedList<>(nodes), "relationships", new SetBackedList<>(rels));
        }
    }
}
