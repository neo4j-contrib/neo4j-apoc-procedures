package apoc.algo.pagerank;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.EntityNotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class PageRankUtils
{
    static final int BATCH_SIZE = 100_000;

    public static int toInt( double value )
    {
        return (int) (100_000 * value);
    }

    public static double toFloat( int value )
    {
        return value / 100_000.0;
    }

    public static int waitForTasks( List<Future> futures )
    {
        int total = 0;
        for ( Future future : futures )
        {
            try
            {
                future.get();
                total++;
            }
            catch ( InterruptedException | ExecutionException e )
            {
                e.printStackTrace();
            }
        }
        futures.clear();
        return total;
    }

    public static void runOperations( ExecutorService pool, final PrimitiveLongIterator it, int totalCount,
            ReadOperations ops, OpsRunner runner )
    {
        List<Future> futures = new ArrayList<>( (int) (totalCount / BATCH_SIZE) );
        while ( it.hasNext() )
        {
            futures.add( pool.submit( new BatchRunnable( ops, it, BATCH_SIZE, runner ) ) );
        }
        PageRankUtils.waitForTasks( futures );
    }
}
