package apoc.periodic;

import apoc.Extended;
import apoc.Pools;
import apoc.util.Util;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.Util.merge;

@Extended
public class PeriodicExtended {
    @Context public GraphDatabaseService db;
    @Context public TerminationGuard terminationGuard;
    @Context public Log log;
    @Context public Pools pools;
    @Context public Transaction tx;

    private void recordError(Map<String, Long> executionErrors, Exception e) {
        String msg = ExceptionUtils.getRootCause(e).getMessage();
        // String msg = ExceptionUtils.getThrowableList(e).stream().map(Throwable::getMessage).collect(Collectors.joining(","))
        executionErrors.compute(msg, (s, i) -> i == null ? 1 : i + 1);
    }

    private void validateQuery(String statement) {
        db.executeTransactionally("EXPLAIN " + statement);
    }

    /**
     * as long as cypherLoop does not return 0, null, false, or the empty string as 'value' do:
     *
     * invoke cypherAction in batched transactions being feeded from cypherIteration running in main thread
     *
     * @param cypherLoop
     * @param cypherIterate
     * @param cypherAction
     * @param batchSize
     */
    @Procedure(mode = Mode.WRITE)
    @Deprecated
    @Description("apoc.periodic.rock_n_roll_while('some cypher for knowing when to stop', 'some cypher for iteration', 'some cypher as action on each iteration', 10000) YIELD batches, total - run the action statement in batches over the iterator statement's results in a separate thread. Returns number of batches and total processed rows")
    public Stream<LoopingBatchAndTotalResult> rock_n_roll_while(
            @Name("cypherLoop") String cypherLoop,
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("batchSize") long batchSize) {
        Map<String, String> fieldStatement = Util.map(
                "cypherLoop", cypherLoop,
                "cypherIterate", cypherIterate);
        validateQueries(fieldStatement);
        Stream<LoopingBatchAndTotalResult> allResults = Stream.empty();

        Map<String,Object> loopParams = new HashMap<>(1);
        Object value = null;

        while (true) {
            loopParams.put("previous", value);

            try (Result result = tx.execute(cypherLoop, loopParams)) {
                value = result.next().get("loop");
                if (!Util.toBoolean(value)) return allResults;
            }

            String periodicId = UUID.randomUUID().toString();
            log.info("starting batched operation using iteration `%s` in separate thread with id: `%s`", cypherIterate, periodicId);
            try (Result result = tx.execute(cypherIterate)) {
                Stream<BatchAndTotalResult> oneResult =
                    iterateAndExecuteBatchedInSeparateThread((int) batchSize, false, false,0, result, (tx, params) -> tx.execute(cypherAction, params), 50, -1, periodicId);
                final Object loopParam = value;
                allResults = Stream.concat(allResults, oneResult.map(r -> r.inLoop(loopParam)));
            }
        }
    }

    private void validateQueries(Map<String, String> fieldStatement) {
        String error = fieldStatement.entrySet()
                .stream()
                .map(e -> {
                    try {
                        validateQuery(e.getValue());
                        return null;
                    } catch (Exception exception) {
                        return String.format("Exception for field `%s`, message: %s", e.getKey(), exception.getMessage());
                    }
                })
                .filter(e -> e != null)
                .collect(Collectors.joining("\n"));
        if (!error.isEmpty()) {
            throw new RuntimeException(error);
        }
    }

    @Deprecated
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.rock_n_roll('some cypher for iteration', 'some cypher as action on each iteration', 10000) YIELD batches, total - run the action statement in batches over the iterator statement's results in a separate thread. Returns number of batches and total processed rows")
    public Stream<BatchAndTotalResult> rock_n_roll(
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("batchSize") long batchSize) {
        Map<String, String> fieldStatement = Util.map(
                "cypherIterate", cypherIterate,
                "cypherAction", cypherAction);
        validateQueries(fieldStatement);

        String periodicId = UUID.randomUUID().toString();
        log.info("starting batched operation using iteration `%s` in separate thread", cypherIterate);
        try (Result result = tx.execute(cypherIterate)) {
            return iterateAndExecuteBatchedInSeparateThread((int)batchSize, false, false, 0, result, (tx, p) -> tx.execute(cypherAction, p), 50, -1, periodicId);
        }
    }

    private Stream<BatchAndTotalResult> iterateAndExecuteBatchedInSeparateThread(int batchsize, boolean parallel, boolean iterateList, long retries,
                  Iterator<Map<String, Object>> iterator, BiConsumer<Transaction, Map<String, Object>> consumer, int concurrency, int failedParams, String periodicId) {

        ExecutorService pool = parallel ? pools.getDefaultExecutorService() : pools.getSingleExecutorService();
        List<Future<Long>> futures = new ArrayList<>(concurrency);
        BatchAndTotalCollector collector = new BatchAndTotalCollector(terminationGuard, failedParams);
        do {
            if (Util.transactionIsTerminated(terminationGuard)) break;
            if (log.isDebugEnabled()) log.debug("Execute, in periodic rock_n_roll with id %s, no %d batch size ", periodicId, batchsize);
            List<Map<String,Object>> batch = Util.take(iterator, batchsize);
            final long currentBatchSize = batch.size();
            Function<Transaction, Long> task;
            if (iterateList) {
                task = txInThread -> {
                    if (Util.transactionIsTerminated(terminationGuard)) return 0L;
                    Map<String, Object> params = Util.map("_count", collector.getCount(), "_batch", batch);
                    long successes = executeAndReportErrors(txInThread, consumer, params, batch, batch.size(), null, collector);
                    return successes;
                };
            } else {
                task = txInThread -> {
                    if (Util.transactionIsTerminated(terminationGuard)) return 0L;
                    AtomicLong localCount = new AtomicLong(collector.getCount());
                    return batch.stream().map(
                            p -> {
                                if (localCount.get() % 1000 == 0 && Util.transactionIsTerminated(terminationGuard)) {
                                    return 0;
                                }
                                Map<String, Object> params = merge(p, Util.map("_count", localCount.get(), "_batch", batch));
                                return executeAndReportErrors(txInThread, consumer, params, batch, 1, localCount, collector);
                            }).mapToLong(n -> (Long) n).sum();
                };
            }
            futures.add(Util.inTxFuture(log, pool, db, task, retries, aLong -> collector.incrementRetried(), _ignored -> collector.incrementBatches()));
            /*  TODO: not sure if the block below is required
            if (futures.size() > concurrency) {
                while (futures.stream().noneMatch(Future::isDone)) { // none done yet, block for a bit
                    LockSupport.parkNanos(1000);
                }
                Iterator<Future<Long>> it = futures.iterator();
                while (it.hasNext()) {
                    Future<Long> future = it.next();
                    if (future.isDone()) {
                        collector.incrementSuccesses(Util.getFuture(future, collector.getBatchErrors(), collector.getFailedBatches(), 0L));
                        it.remove();
                    }
                }
            }*/
            collector.incrementCount(currentBatchSize);
            if (log.isDebugEnabled()) {
                log.debug("Processed, in periodic rock_n_roll with id %s, %d iterations of %d total", periodicId, batchsize, collector.getCount());
            }
        } while (iterator.hasNext());

        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
        ToLongFunction<Future<Long>> toLongFunction = wasTerminated ?
                f -> Util.getFutureOrCancel(f, collector.getBatchErrors(), collector.getFailedBatches(), 0L) :
                f -> Util.getFuture(f, collector.getBatchErrors(), collector.getFailedBatches(), 0L);
        collector.incrementSuccesses(futures.stream().mapToLong(toLongFunction).sum());

        Util.logErrors("Error during iterate.commit:", collector.getBatchErrors(), log);
        Util.logErrors("Error during iterate.execute:", collector.getOperationErrors(), log);
        if (log.isDebugEnabled()) {
            log.debug("Terminated periodic rock_n_roll with id %s with %d executions", periodicId, collector.getCount());
        }
        return Stream.of(collector.getResult());
    }

    private long executeAndReportErrors(Transaction tx, BiConsumer<Transaction, Map<String, Object>> consumer, Map<String, Object> params,
                                        List<Map<String, Object>> batch, int returnValue, AtomicLong localCount, BatchAndTotalCollector collector) {
        try {
            consumer.accept(tx, params);
            if (localCount!=null) {
                localCount.getAndIncrement();
            }
            return returnValue;
        } catch (Exception e) {
            collector.incrementFailedOps(batch.size());
            collector.amendFailedParamsMap(batch);
            recordError(collector.getOperationErrors(), e);
            throw e;
        }
    }

}
