package apoc.dv;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class NodeAndRelResult {
    public final Node node;
    public final Relationship relationship;

    public NodeAndRelResult(Node node, Relationship relationship) {
        this.node = node;
        this.relationship = relationship;
    }
}
