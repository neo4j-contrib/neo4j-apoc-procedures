package apoc.refactor;

import org.neo4j.graphdb.Relationship;

/**
 * @author mh
 * @since 25.03.16
 */
public class RelationshipRefactorResult {
    public long input;
    public Relationship output;
    public String error;

    public RelationshipRefactorResult(Long id) {
        this.input = id;
    }

    public RelationshipRefactorResult withError(Exception e) {
        this.error = e.getMessage();
        return this;
    }

    public RelationshipRefactorResult withError(String message) {
        this.error = message;
        return this;
    }

    public RelationshipRefactorResult withOther(Relationship rel) {
        this.output = rel;
        return this;
    }
}
