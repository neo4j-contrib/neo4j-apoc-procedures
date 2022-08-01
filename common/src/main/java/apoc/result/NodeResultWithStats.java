package apoc.result;

import org.neo4j.graphdb.Node;

import java.util.Map;


public class NodeResultWithStats extends NodeResult {
    public final Map<String, Object> stats;

    public NodeResultWithStats(Node node, Map<String, Object> stats) {
        super(node);
        this.stats = stats;
    }
}
