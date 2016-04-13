package apoc.result;

import org.neo4j.graphdb.Relationship;

/**
 * @author mh
 * @since 26.02.16
 */
public class WeightedRelationshipResult {
    public final Relationship rel;
    public final double weight;

    public WeightedRelationshipResult(Relationship rel, double weight) {
        this.rel = rel;
        this.weight = weight;
    }
}
