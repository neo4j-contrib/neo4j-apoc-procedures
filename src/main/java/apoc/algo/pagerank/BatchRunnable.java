package apoc.algo.pagerank;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static apoc.algo.pagerank.PageRankUtils.ctx;

public class BatchRunnable implements Runnable, OpsRunner
{
    final long[] ids;
    private final OpsRunner runner;
    private final GraphDatabaseAPI api;
    int offset = 0;

    public BatchRunnable(final GraphDatabaseAPI api, PrimitiveLongIterator iterator, int batchSize, OpsRunner runner )
    {
        this.api = api;
        ids = add( iterator, batchSize );
        this.runner = runner;
    }

    private long[] add( PrimitiveLongIterator it, int count )
    {
        long[] ids = new long[count];
        while ( count-- > 0 && it.hasNext() )
        {
            ids[offset++] = it.next();
        }
        return ids;
    }

    public void run()
    {
        try (Transaction tx = api.beginTx()) {
            ReadOperations ops = ctx(api).get().readOperations();
            int notFound = 0;
            for (int i = 0; i < offset; i++) {
                try {
                    run(ops, (int) ids[i]);
                } catch (EntityNotFoundException e) {
                    notFound++;
                }
            }
            if (notFound > 0) {
                System.err.println("Entities not found " + notFound);
            }
            tx.success();
        }
    }

    @Override
    public void run( ReadOperations ops, int node ) throws EntityNotFoundException
    {
        runner.run( ops, node );
    }
}
