package apoc.periodic;

import org.neo4j.procedure.*;
import apoc.Pools;
import apoc.util.Util;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static apoc.util.Util.merge;
import static java.lang.System.nanoTime;
import static java.util.Collections.singletonMap;

public class Periodic {

    @Context public GraphDatabaseAPI db;

    @Context public Log log;

    final static Map<JobInfo,Future> list = new ConcurrentHashMap<>();
    static {
        Runnable runnable = () -> {
            for (Iterator<Map.Entry<JobInfo, Future>> it = list.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<JobInfo, Future> entry = it.next();
                if (entry.getValue().isDone() || entry.getValue().isCancelled()) it.remove();
            }
        };
        Pools.SCHEDULED.scheduleAtFixedRate(runnable,10,10,TimeUnit.SECONDS);
    }

    @Context
    public KernelTransaction tx;

    @Procedure
    @Description("apoc.periodic.list - list all jobs")
    public Stream<JobInfo> list() {
        return list.entrySet().stream().map( (e) -> e.getKey().update(e.getValue()));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.commit(statement,params) - runs the given statement in separate transactions until it returns 0")
    public Stream<RundownResult> commit(@Name("statement") String statement, @Name("params") Map<String,Object> parameters) throws ExecutionException, InterruptedException {
        Map<String,Object> params = parameters == null ? Collections.emptyMap() : parameters;
        long total = 0, executions = 0, updates = 0;
        long start = nanoTime();

        AtomicInteger batches = new AtomicInteger();
        AtomicInteger failedCommits = new AtomicInteger();
        Map<String,Long> commitErrors = new ConcurrentHashMap<>();
        AtomicInteger failedBatches = new AtomicInteger();
        Map<String,Long> batchErrors = new ConcurrentHashMap<>();

        do {
            Map<String, Object> window = Util.map("_count", updates, "_total", total);
            updates = Util.getFuture(Pools.SCHEDULED.submit(() -> {
                batches.incrementAndGet();
                try {
                    return executeNumericResultStatement(statement, merge(window, params));
                } catch(Exception e) {
                    failedBatches.incrementAndGet();
                    recordError(batchErrors, e);
                    return 0L;
                }
            }), commitErrors, failedCommits, 0L);
            total += updates;
            if (updates > 0) executions++;
        } while (updates > 0);
        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(nanoTime() - start);
        return Stream.of(new RundownResult(total,executions, timeTaken, batches.get(),failedBatches.get(),batchErrors, failedCommits.get(), commitErrors));
    }

    private void recordError(Map<String, Long> executionErrors, Exception e) {
        executionErrors.compute(e.getMessage(),(s,i) -> i == null ? 1 : i + 1);
    }

    public static class RundownResult {
        public final long updates;
        public final long executions;
        public final long runtime;
        public final long batches;
        public final long faileBatches;
        public final Map<String, Long> batchErrors;
        public final long failedCommits;
        public final Map<String, Long> commitErrors;

        public RundownResult(long total, long executions, long timeTaken, long batches, long faileBatches, Map<String, Long> batchErrors, long failedCommits, Map<String, Long> commitErrors) {
            this.updates = total;
            this.executions = executions;
            this.runtime = timeTaken;
            this.batches = batches;
            this.faileBatches = faileBatches;
            this.batchErrors = batchErrors;
            this.failedCommits = failedCommits;
            this.commitErrors = commitErrors;
        }
    }

    private long executeNumericResultStatement(@Name("statement") String statement, @Name("params") Map<String, Object> parameters) {
        long sum = 0;
        try (Result result = db.execute(statement, parameters)) {
            while (result.hasNext()) {
                Collection<Object> row = result.next().values();
                for (Object value : row) {
                    if (value instanceof Number) {
                        sum += ((Number)value).longValue();
                    }
                }
            }
        }
        return sum;
    }

    @Procedure
    @Description("apoc.periodic.cancel(name) - cancel job with the given name")
    public Stream<JobInfo> cancel(@Name("name") String name) {
        JobInfo info = new JobInfo(name);
        Future future = list.remove(info);
        if (future != null) {
            future.cancel(true);
            return Stream.of(info.update(future));
        }
        return Stream.empty();
    }

    @Procedure
    @Description("apoc.periodic.submit('name',statement) - submit a one-off background statement")
    public Stream<JobInfo> submit(@Name("name") String name, @Name("statement") String statement) {
        JobInfo info = submit(name, () -> {
            try {
                Iterators.count(db.execute(statement));
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        });
        return Stream.of(info);
    }

    @Procedure
    @Description("apoc.periodic.repeat('name',statement,repeat-rate-in-seconds) submit a repeatedly-called background statement")
    public Stream<JobInfo> repeat(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate) {
        JobInfo info = schedule(name, () -> Iterators.count(db.execute(statement)),0,rate);
        return Stream.of(info);
    }

    // TODO
    @Description("apoc.periodic.countdown('name',statement,repeat-rate-in-seconds) submit a repeatedly-called background statement until it returns 0")
    public Stream<JobInfo> countdown(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate) {
        JobInfo info = submit(name, new Countdown(name, statement, rate));
        return Stream.of(info);
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public static <T> JobInfo submit(String name, Runnable task) {
        JobInfo info = new JobInfo(name);
        Future<T> future = list.remove(info);
        if (future != null && !future.isDone()) future.cancel(false);

        Future newFuture = Pools.SCHEDULED.submit(task);
        list.put(info,newFuture);
        return info;
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public static JobInfo schedule(String name, Runnable task, long delay, long repeat) {
        JobInfo info = new JobInfo(name,delay,repeat);
        Future future = list.remove(info);
        if (future != null && !future.isDone()) future.cancel(false);

        ScheduledFuture<?> newFuture = Pools.SCHEDULED.scheduleWithFixedDelay(task, delay, repeat, TimeUnit.SECONDS);
        list.put(info,newFuture);
        return info;
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
    @Description("apoc.periodic.rock_n_roll_while('some cypher for knowing when to stop', 'some cypher for iteration', 'some cypher as action on each iteration', 10000) YIELD batches, total - run the action statement in batches over the iterator statement's results in a separate thread. Returns number of batches and total processed rows")
    public Stream<LoopingBatchAndTotalResult> rock_n_roll_while(
            @Name("cypherLoop") String cypherLoop,
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("batchSize") long batchSize) {

        Stream<LoopingBatchAndTotalResult> allResults = Stream.empty();

        Map<String,Object> loopParams = new HashMap<>(1);
        Object value = null;

        while (true) {
            loopParams.put("previous", value);

            try (Result result = db.execute(cypherLoop, loopParams)) {
                value = result.next().get("loop");
                if (!Util.toBoolean(value)) return allResults;
            }

            log.info("starting batched operation using iteration `%s` in separate thread", cypherIterate);
            try (Result result = db.execute(cypherIterate)) {
                Stream<BatchAndTotalResult> oneResult =
                    iterateAndExecuteBatchedInSeparateThread((int) batchSize, false, false,0, result, params -> db.execute(cypherAction, params));
                final Object loopParam = value;
                allResults = Stream.concat(allResults, oneResult.map(r -> r.inLoop(loopParam)));
            }
        }
    }

    /**
     * invoke cypherAction in batched transactions being feeded from cypherIteration running in main thread
     * @param cypherIterate
     * @param cypherAction
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.iterate('statement returning items', 'statement per item', {batchSize:1000,iterateList:false,parallel:true}) YIELD batches, total - run the second statement for each item returned by the first statement. Returns number of batches and total processed rows")
    public Stream<BatchAndTotalResult> iterate(
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("config") Map<String,Object> config) {

        long batchSize = Util.toLong(config.getOrDefault("batchSize", 10000));
        boolean parallel = Util.toBoolean(config.getOrDefault("parallel", false));
        boolean iterateList = Util.toBoolean(config.getOrDefault("iterateList", false));
        long retries = Util.toLong(config.getOrDefault("retries", 0)); // todo sleep/delay or push to end of batch to try again or immediate ?
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        try (Result result = db.execute(cypherIterate,params)) {
            String innerStatement = prepareInnerStatement(cypherAction, iterateList, result.columns(), "_batch");
            log.info("starting batching from `%s` operation using iteration `%s` in separate thread", cypherIterate,cypherAction);
            return iterateAndExecuteBatchedInSeparateThread((int)batchSize, parallel, iterateList, retries, result, (p) -> db.execute(innerStatement, merge(params, p)).close());
        }
    }

    public long retry(Consumer<Map<String, Object>> executor, Map<String, Object> params, long retry, long maxRetries) {
        try {
            executor.accept(merge(params, singletonMap("_retry", retry)));
            return retry;
        } catch (Exception e) {
            if (retry >= maxRetries) throw e;
            log.warn("Retrying operation "+retry+" of "+maxRetries);
            Util.sleep(100);
            return retry(executor, params, retry + 1, maxRetries);
        }
    }


    static Pattern CONTAINS_PARAM_MAPPING = Pattern.compile("(WITH|UNWIND)\\s*[{$]",Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
    public String prepareInnerStatement(String cypherAction, boolean iterateList, List<String> columns, String iterator) {
        if (CONTAINS_PARAM_MAPPING.matcher(cypherAction).find()) return cypherAction;
        if (iterateList) {
            String with = Util.withMapping(columns.stream(), (c) -> Util.quote(iterator) + "." + Util.quote(c) + " AS " + Util.quote(c));
            return "UNWIND "+ Util.param(iterator)+" AS "+ Util.quote(iterator) + with + " " + cypherAction;
        }
        return Util.withMapping(columns.stream(), (c) ->  Util.param(c) + " AS " + Util.quote(c)) + cypherAction;
    }


    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.rock_n_roll('some cypher for iteration', 'some cypher as action on each iteration', 10000) YIELD batches, total - run the action statement in batches over the iterator statement's results in a separate thread. Returns number of batches and total processed rows")
    public Stream<BatchAndTotalResult> rock_n_roll(
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("batchSize") long batchSize) {

        log.info("starting batched operation using iteration `%s` in separate thread", cypherIterate);
        try (Result result = db.execute(cypherIterate)) {
            return iterateAndExecuteBatchedInSeparateThread((int)batchSize, false, false, 0, result, p -> db.execute(cypherAction, p).close());
        }
    }

    private Stream<BatchAndTotalResult> iterateAndExecuteBatchedInSeparateThread(int batchsize, boolean parallel, boolean iterateList, long retries,
                                                                                 Iterator<Map<String,Object>> iterator, Consumer<Map<String,Object>> consumer) {
        ExecutorService pool = parallel ? Pools.DEFAULT : Pools.SINGLE;
        List<Future<Long>> futures = new ArrayList<>(1000);
        long batches = 0;
        long start = System.nanoTime();
        AtomicLong count = new AtomicLong();
        AtomicInteger failedOps = new AtomicInteger();
        AtomicLong retried = new AtomicLong();
        Map<String,Long> operationErrors = new ConcurrentHashMap<>();
        do {
            if (log.isDebugEnabled()) log.debug("execute in batch no " + batches + " batch size " + batchsize);
            List<Map<String,Object>> batch = Util.take(iterator, batchsize);
            long currentBatchSize = batch.size();
            Callable<Long> task;
            if (iterateList) {
                task = () -> {
                    long c = count.addAndGet(currentBatchSize);
                    List<Map<String,Object>> batchLocal = batch;
                    try {
                        Map<String, Object> params = Util.map("_count", c, "_batch", batchLocal);
                        retried.addAndGet(retry(consumer,params,0,retries));
                    } catch (Exception e) {
                        failedOps.addAndGet(batchsize);
                        recordError(operationErrors, e);
                    }
                    return currentBatchSize;
                };
            } else {
                task = () -> batch.stream().map(
                        p -> {
                            long c = count.incrementAndGet();
                            List<Map<String,Object>> batchLocal = batch;
                            try {
                                Map<String, Object> params = merge(p, Util.map("_count", c, "_batch", batchLocal));
                                retried.addAndGet(retry(consumer,params,0,retries));
                            } catch (Exception e) {
                                failedOps.incrementAndGet();
                                recordError(operationErrors, e);
                            }
                            return 1;
                        }).mapToLong(l -> l).sum();
            }
            futures.add(Util.inTxFuture(pool, db, task));
            batches++;
        } while (iterator.hasNext());

        AtomicInteger failedBatches = new AtomicInteger();
        Map<String,Long> batchErrors = new HashMap<>();
        long successes = futures.stream().mapToLong(f -> Util.getFuture(f, batchErrors, failedBatches, 0L)).sum();
        Util.logErrors("Error during iterate.commit:", batchErrors, log);
        Util.logErrors("Error during iterate.execute:", operationErrors, log);
        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        BatchAndTotalResult result =
                new BatchAndTotalResult(batches, count.get(), timeTaken, successes, failedOps.get(), failedBatches.get(), retried.get(), operationErrors, batchErrors);
        return Stream.of(result);
    }

    public static class BatchAndTotalResult {
        public final long batches;
        public final long total;
        public final long timeTaken;
        public final long committedOperations;
        public final long failedOperations;
        public final long failedBatches;
        public final long retries;
        public final Map<String,Long> errorMessages;
        public final Map<String,Object> batch;
        public final Map<String,Object> operations;

        public BatchAndTotalResult(long batches, long total, long timeTaken, long committedOperations, long failedOperations, long failedBatches,long retries, Map<String, Long> operationErrors, Map<String, Long> batchErrors) {
            this.batches = batches;
            this.total = total;
            this.timeTaken = timeTaken;
            this.committedOperations = committedOperations;
            this.failedOperations = failedOperations;
            this.failedBatches = failedBatches;
            this.retries = retries;
            this.errorMessages = operationErrors;
            this.batch = Util.map("total",batches,"failed",failedBatches,"committed",batches-failedBatches,"errors",batchErrors);
            this.operations = Util.map("total",total,"failed",failedOperations,"committed", committedOperations,"errors",operationErrors);
        }

        public LoopingBatchAndTotalResult inLoop(Object loop) {
            return new LoopingBatchAndTotalResult(loop, batches, total);
        }
    }

    public static class LoopingBatchAndTotalResult {
        public Object loop;
        public long batches;
        public long total;

        public LoopingBatchAndTotalResult(Object loop, long batches, long total) {
            this.loop = loop;
            this.batches = batches;
            this.total = total;
        }
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public static JobInfo schedule(String name, Runnable task, long delay) {
        JobInfo info = new JobInfo(name,delay,0);
        Future future = list.remove(info);
        if (future != null) future.cancel(false);

        ScheduledFuture<?> newFuture = Pools.SCHEDULED.schedule(task, delay, TimeUnit.SECONDS);
        list.put(info,newFuture);
        return info;
    }

    public static class JobInfo {
        public final String name;
        public long delay;
        public long rate;
        public boolean done;
        public boolean cancelled;

        public JobInfo(String name) {
            this.name = name;
        }

        public JobInfo(String name, long delay, long rate) {
            this.name = name;
            this.delay = delay;
            this.rate = rate;
        }

        public JobInfo update(Future future) {
            this.done = future.isDone();
            this.cancelled = future.isCancelled();
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof JobInfo && name.equals(((JobInfo) o).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    private class Countdown implements Runnable {
        private final String name;
        private final String statement;
        private final long rate;

        public Countdown(String name, String statement, long rate) {
            this.name = name;
            this.statement = statement;
            this.rate = rate;
        }

        @Override
        public void run() {
            if (Periodic.this.executeNumericResultStatement(statement, null) > 0) {
                Pools.SCHEDULED.schedule(() -> submit(name, this), rate, TimeUnit.SECONDS);
            }
        }
    }
}
