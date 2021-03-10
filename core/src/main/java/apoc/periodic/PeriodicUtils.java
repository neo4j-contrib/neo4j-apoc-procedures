package apoc.periodic;

import apoc.Pools;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PeriodicUtils {

    private PeriodicUtils() {

    }

    public static Pair<String,Boolean> prepareInnerStatement(String cypherAction, BatchMode batchMode, List<String> columns, String iteratorVariableName) {
        String names = columns.stream().map(Util::quote).collect(Collectors.joining("|"));
        boolean withCheck = regNoCaseMultiLine("[{$](" + names + ")\\}?\\s+AS\\s+").matcher(cypherAction).find();
        if (withCheck) return Pair.of(cypherAction, false);

        switch(batchMode) {
            case SINGLE:
                 return Pair.of(Util.withMapping(columns.stream(), (c) ->  Util.param(c) + " AS " + Util.quote(c)) + cypherAction,false);
            case BATCH:
                if (regNoCaseMultiLine("UNWIND\\s+[{$]" + iteratorVariableName+"\\}?\\s+AS\\s+").matcher(cypherAction).find()) {
                    return Pair.of(cypherAction, true);
                }
                String with = Util.withMapping(columns.stream(), (c) -> Util.quote(iteratorVariableName) + "." + Util.quote(c) + " AS " + Util.quote(c));
                return Pair.of("UNWIND "+ Util.param(iteratorVariableName)+" AS "+ Util.quote(iteratorVariableName) + with + " " + cypherAction,true);
            case BATCH_SINGLE:
                return Pair.of(cypherAction, true);
            default:
                throw new IllegalArgumentException("Unrecognised batch mode: [" + batchMode + "]");
        }
    }
    
    public static Pattern regNoCaseMultiLine(String pattern) {
        return Pattern.compile(pattern,Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
    }

    public static Stream<BatchAndTotalResult> iterateAndExecuteBatchedInSeparateThread(
            GraphDatabaseService db, TerminationGuard terminationGuard, Log log, Pools pools,
            int batchsize, boolean parallel, boolean iterateList, long retries,
            Iterator<Map<String, Object>> iterator, BiFunction<Transaction, Map<String, Object>, QueryStatistics> consumer,
            int concurrency, int failedParams) {

        ExecutorService pool = parallel ? pools.getDefaultExecutorService() : pools.getSingleExecutorService();
        List<Future<Long>> futures = new ArrayList<>(concurrency);
        BatchAndTotalCollector collector = new BatchAndTotalCollector(terminationGuard, failedParams);
        AtomicInteger activeFutures = new AtomicInteger(0);

        do {
            if (Util.transactionIsTerminated(terminationGuard)) break;

            if (activeFutures.get() < concurrency || !parallel) {
                // we have capacity, add a new Future to the list
                activeFutures.incrementAndGet();

                if (log.isDebugEnabled()) log.debug("execute in batch no %d batch size ", batchsize);
                List<Map<String,Object>> batch = Util.take(iterator, batchsize);
                final long currentBatchSize = batch.size();
                Periodic.ExecuteBatch executeBatch =
                        iterateList ?
                                new Periodic.ListExecuteBatch(terminationGuard, collector, batch, consumer) :
                                new Periodic.OneByOneExecuteBatch(terminationGuard, collector, batch, consumer);

                futures.add(Util.inTxFuture(log,
                        pool,
                        db,
                        executeBatch,
                        retries,
                        retryCount -> collector.incrementRetried(),
                        onComplete -> {
                            collector.incrementBatches();
                            executeBatch.release();
                            activeFutures.decrementAndGet();
                        }));
                collector.incrementCount(currentBatchSize);
            } else {
                // we can't block until the counter decrease as we might miss a cancellation, so
                // let this thread be preempted for a bit before we check for cancellation or
                // capacity.
                LockSupport.parkNanos(1000);
            }
        } while (iterator.hasNext());

        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
        ToLongFunction<Future<Long>> toLongFunction = wasTerminated ?
                f -> Util.getFutureOrCancel(f, collector.getBatchErrors(), collector.getFailedBatches(), 0L) :
                f -> Util.getFuture(f, collector.getBatchErrors(), collector.getFailedBatches(), 0L);
        collector.incrementSuccesses(futures.stream().mapToLong(toLongFunction).sum());

        Util.logErrors("Error during iterate.commit:", collector.getBatchErrors(), log);
        Util.logErrors("Error during iterate.execute:", collector.getOperationErrors(), log);
        return Stream.of(collector.getResult());
    }
}

/*
a batchMode variable where:
* single -> call 2nd statement individually but in one tx (currently iterateList: false)
* batch -> prepend UNWIND _batch to 2nd statement (currently iterateList: true)
* batch_single -> pass _batch through to 2nd statement
 */

