package apoc.result;

import org.neo4j.graphdb.Node;

/**
 * @author mh
 * @since 26.02.16
 */
public class NodeResult {
    public final Node node;

    public NodeResult(Node node) {
        this.node = node;
    }
}
