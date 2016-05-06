package apoc.algo.pagerank;

public interface Algorithm
{

    void compute( int iterations );

    double getResult( long node );

    long numberOfNodes();

    String getPropertyName();

}