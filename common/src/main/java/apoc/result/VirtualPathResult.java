package apoc.result;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * @author mh
 * @since 26.02.16
 */
public class VirtualPathResult {
    public final Node from;
    public final Relationship rel;
    public final Node to;

    public VirtualPathResult(Node from, Relationship rel, Node to) {
        this.from = from;
        this.rel = rel;
        this.to = to;
    }
}
