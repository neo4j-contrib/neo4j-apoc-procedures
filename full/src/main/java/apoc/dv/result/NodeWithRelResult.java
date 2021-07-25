package apoc.dv.result;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class NodeWithRelResult {
    public final Node node;
    public final Relationship relationship;

    public NodeWithRelResult(Node node, Relationship relationship) {
        this.node = node;
        this.relationship = relationship;
    }
}
