package apoc.result;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * @author mh
 * @since 26.02.16
 */
public class PathResult {
    public final Node from;
    public final Relationship rel;
    public final Node to;

    public PathResult(Node from, Relationship rel, Node to) {
        this.from = from;
        this.rel = rel;
        this.to = to;
    }
}
