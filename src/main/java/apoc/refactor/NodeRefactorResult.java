package apoc.refactor;

import org.neo4j.graphdb.Node;

/**
 * @author mh
 * @since 25.03.16
 */
public class NodeRefactorResult {
    public long input;
    public Node output;
    public String error;

    public NodeRefactorResult(Long id) {
        this.input = id;
    }

    public NodeRefactorResult withError(Exception e) {
        this.error = e.getMessage();
        return this;
    }

    public NodeRefactorResult withOther(Node rel) {
        this.output = rel;
        return this;
    }
}
