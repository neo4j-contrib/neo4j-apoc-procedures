package apoc.algo.pagerank;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    public static void writeBackResults(ExecutorService pool, GraphDatabaseAPI db, int [] nodes, Algorithm algorithm,
                                        int batchSize) {
        ThreadToStatementContextBridge ctx = db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
        int propertyNameId;
        try (Transaction tx = db.beginTx()) {
            propertyNameId = ctx.get().tokenWriteOperations().propertyKeyGetOrCreateForName(algorithm.getPropertyName());
            tx.success();
        } catch (IllegalTokenNameException e) {
            throw new RuntimeException(e);
        }
        final long totalNodes = algorithm.numberOfNodes();
        int batches = (int) totalNodes / batchSize;
        List<Future> futures = new ArrayList<>(batches);
        for (int i = 0; i < totalNodes; i += batchSize) {
            int nodeIndex = i;
            final int start = nodeIndex;
            Future future = pool.submit(new Runnable() {
                public void run() {
                    try (Transaction tx = db.beginTx()) {
                        DataWriteOperations ops = ctx.get().dataWriteOperations();
                        for (long i = 0; i < batchSize; i++) {
                            long nodeIndex = i + start;
                            if (nodeIndex >= totalNodes) break;

                            int graphNode = nodes[(int)nodeIndex];
                            double value = algorithm.getResult(graphNode);
                            if (value > 0) {
                                ops.nodeSetProperty(graphNode, DefinedProperty.doubleProperty(propertyNameId, value));
                            }
                        }
                        tx.success();
                    } catch (ConstraintValidationKernelException | InvalidTransactionTypeKernelException |
                            EntityNotFoundException | AutoIndexingKernelException |
                            org.neo4j.kernel.api.exceptions.EntityNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            });

            futures.add(future);
        }
        PageRankUtils.waitForTasks(futures);
    }

}
