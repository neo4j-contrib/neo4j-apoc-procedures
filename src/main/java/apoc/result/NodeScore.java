package apoc.result;

import org.neo4j.graphdb.Node;

/**
 * @author mh
 * @since 12.05.16
 */
public class NodeScore {
    public final Node node;
    public final Double score;

    public NodeScore(Node node, Double score) {
        this.node = node;
        this.score = score;
    }
}
