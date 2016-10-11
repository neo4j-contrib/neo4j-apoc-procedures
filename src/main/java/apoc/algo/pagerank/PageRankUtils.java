package apoc.algo.pagerank;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
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

    public static void runOperations(ExecutorService pool, final PrimitiveLongIterator it, int totalCount,
                                     final GraphDatabaseAPI api, OpsRunner runner )
    {
        List<Future> futures = new ArrayList<>( totalCount / BATCH_SIZE + 1);
        while ( it.hasNext() )
        {
            futures.add( pool.submit( new BatchRunnable( api, it, BATCH_SIZE, runner ) ) );
        }
        PageRankUtils.waitForTasks( futures );
    }
    public static void runOperations(ExecutorService pool, List<BatchRunnable> runners )
    {
        List<Future> futures = new ArrayList<>( runners.size() );
        for (BatchRunnable runnable : runners) {
            futures.add( pool.submit( runnable ) );
        }
        PageRankUtils.waitForTasks( futures );
    }

    public static List<BatchRunnable> prepareOperations(final PrimitiveLongIterator it, int totalCount,
                                                        final GraphDatabaseAPI api, OpsRunner runner )
    {
        List<BatchRunnable> runners = new ArrayList<>( (int) (totalCount / BATCH_SIZE) + 1);
        while ( it.hasNext() )
        {
            runners.add( new BatchRunnable( api, it, BATCH_SIZE, runner ) );
        }
        return runners;
    }

    public static ThreadToStatementContextBridge ctx(GraphDatabaseAPI db) {
        return db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }
}
