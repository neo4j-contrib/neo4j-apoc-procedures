package apoc.result;

import org.neo4j.graphdb.Node;

/**
 * @author mh
 * @since 26.02.16
 */
public class WeightedNodeResult {
    public final Node node;
    public final double weight;

    public WeightedNodeResult(Node node, double weight) {
        this.weight = weight;
        this.node = node;
    }
}
