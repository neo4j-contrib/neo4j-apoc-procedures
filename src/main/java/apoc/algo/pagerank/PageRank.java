package apoc.algo.pagerank;

import org.neo4j.graphdb.RelationshipType;

public interface PageRank extends Algorithm
{
    double ALPHA = 0.85;
    double ONE_MINUS_ALPHA = 1 - ALPHA;

    void compute( int iterations, RelationshipType... relationshipTypes );

    double getResult( long node );

    long numberOfNodes();

    String getPropertyName();
}