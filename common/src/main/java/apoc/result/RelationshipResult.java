package apoc.result;

import org.neo4j.graphdb.Relationship;

/**
 * @author mh
 * @since 26.02.16
 */
public class RelationshipResult {
    public final Relationship rel;

    public RelationshipResult(Relationship rel) {
        this.rel = rel;
    }
}
