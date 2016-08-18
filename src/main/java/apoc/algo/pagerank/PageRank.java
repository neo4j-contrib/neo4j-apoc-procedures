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

    PageRankStatistics getStatistics();

    class PageRankStatistics {
        public long nodes, relationships, iterations, readNodeMillis, readRelationshipMillis,computeMillis,writeMillis;
        public boolean write;

        public PageRankStatistics(long nodes, long relationships, long iterations, long readNodeMillis, long readRelationshipMillis, long computeMillis, long writeMillis, boolean write) {
            this.nodes = nodes;
            this.relationships = relationships;
            this.iterations = iterations;
            this.readNodeMillis = readNodeMillis;
            this.readRelationshipMillis = readRelationshipMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.write = write;
        }

        public PageRankStatistics() {
        }
    }

}
