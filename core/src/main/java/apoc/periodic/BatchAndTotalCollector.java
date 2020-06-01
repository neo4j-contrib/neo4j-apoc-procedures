package apoc.periodic;

import apoc.util.Util;
import org.neo4j.procedure.TerminationGuard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BatchAndTotalCollector {
    private final int failedParams;
    private long start = System.nanoTime();
    private AtomicLong batches = new AtomicLong();
    private long successes = 0;
    private AtomicLong count = new AtomicLong();
    private AtomicLong failedOps = new AtomicLong();
    private AtomicLong retried = new AtomicLong();
    private Map<String, Long> operationErrors = new ConcurrentHashMap<>();
    private AtomicInteger failedBatches = new AtomicInteger();
    private Map<String, Long> batchErrors = new HashMap<>();
    private Map<String, List<Map<String, Object>>> failedParamsMap = new ConcurrentHashMap<>();
    private final boolean wasTerminated;

    public BatchAndTotalCollector(TerminationGuard terminationGuard, int failedParams) {
        this.failedParams = failedParams;
        wasTerminated = Util.transactionIsTerminated(terminationGuard);
    }

    public BatchAndTotalResult getResult() {
        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        return new BatchAndTotalResult(batches.get(), count.get(), timeTaken, successes, failedOps.get(), failedBatches.get(), retried.get(), operationErrors, batchErrors, wasTerminated, failedParamsMap);
    }

    public long getBatches() {
        return batches.get();
    }

    public long getCount() {
        return count.get();
    }

    public void incrementFailedOps(long size) {
        failedOps.addAndGet(size);
    }

    public void incrementBatches() {
        batches.incrementAndGet();
    }

    public void incrementSuccesses(long increment) {
        successes += increment;
    }

    public void incrementCount(long currentBatchSize) {
        count.addAndGet(currentBatchSize);
    }

    public Map<String, Long> getBatchErrors() {
        return batchErrors;
    }

    public Map<String, Long> getOperationErrors() {
        return operationErrors;
    }

    public Map<String, List<Map<String, Object>>> getFailedParamsMap() {
        return failedParamsMap;
    }

    public void amendFailedParamsMap(List<Map<String, Object>> batch) {
        if (failedParams >= 0) {
            failedParamsMap.put(
                    Long.toString(batches.get()),
                    new ArrayList<>(batch.subList(0, Math.min(failedParams + 1, batch.size())))
            );
        }
    }

    public AtomicInteger getFailedBatches() {
        return failedBatches;
    }

    public void incrementRetried() {
        retried.incrementAndGet();
    }
}
