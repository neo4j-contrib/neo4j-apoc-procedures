package apoc.result;

import org.neo4j.graphdb.Relationship;

import java.util.Map;


public class RelationshipResultWithStats extends RelationshipResult {
    public final Map<String, Object> stats;

    public RelationshipResultWithStats(Relationship relationship, Map<String, Object> stats) {
        super(relationship);
        this.stats = stats;
    }
}
