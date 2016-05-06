package apoc.algo.pagerank;

public interface PageRank extends Algorithm
{
    double ALPHA = 0.85;
    double ONE_MINUS_ALPHA = 1 - ALPHA;

    void compute( int iterations );

    double getResult( long node );

    long numberOfNodes();

    String getPropertyName();
}