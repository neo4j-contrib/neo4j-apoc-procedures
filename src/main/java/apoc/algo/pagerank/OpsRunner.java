package apoc.algo.pagerank;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public interface OpsRunner
{
    void run(ReadOperations ops, int node ) throws EntityNotFoundException;
}
