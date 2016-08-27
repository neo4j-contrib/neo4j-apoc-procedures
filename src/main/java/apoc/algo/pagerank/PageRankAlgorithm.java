package apoc.algo.pagerank;

import org.neo4j.graphdb.RelationshipType;

public interface PageRankAlgorithm
{

    void compute( int iterations, RelationshipType... relationshipTypes );

    double getResult( long node );

    long numberOfNodes();

    String getPropertyName();

}