package apoc.algo.pagerank;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public interface OpsRunner
{
    void run( int node ) throws EntityNotFoundException;
}