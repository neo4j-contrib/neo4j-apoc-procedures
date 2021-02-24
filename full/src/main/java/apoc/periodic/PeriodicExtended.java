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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

            log.info("starting batched operation using iteration `%s` in separate thread", cypherIterate);
            try (Result result = tx.execute(cypherIterate)) {
                Stream<BatchAndTotalResult> oneResult =
                    PeriodicUtils.iterateAndExecuteBatchedInSeparateThread(
                            db, terminationGuard, log, pools,
                            (int) batchSize, false, false, 0,
                            result, (tx, params) -> tx.execute(cypherAction, params).getQueryStatistics(), 50, -1);
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

        log.info("starting batched operation using iteration `%s` in separate thread", cypherIterate);
        try (Result result = tx.execute(cypherIterate)) {
            return PeriodicUtils.iterateAndExecuteBatchedInSeparateThread(
                    db, terminationGuard, log, pools,
                    (int)batchSize, false, false, 0, result,
                    (tx, p) -> tx.execute(cypherAction, p).getQueryStatistics(), 50, -1);
        }
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
