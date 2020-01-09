package apoc.periodic;

import apoc.Pools;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.Util.merge;
import static java.lang.System.nanoTime;
import static java.util.Collections.singletonMap;

public class Periodic {

    public static final Pattern RUNTIME_PATTERN = Pattern.compile("\\bruntime\\s*=", Pattern.CASE_INSENSITIVE);
    public static final Pattern CYPHER_PREFIX_PATTERN = Pattern.compile("\\bcypher\\b", Pattern.CASE_INSENSITIVE);
    public static final String CYPHER_RUNTIME_SLOTTED = "cypher runtime=slotted ";
    @Context public GraphDatabaseService db;
    @Context public TerminationGuard terminationGuard;

    @Context public Log log;

    final static Map<JobInfo,Future> list = new ConcurrentHashMap<>();

    final static Pattern LIMIT_PATTERN = Pattern.compile("\\slimit\\s", Pattern.CASE_INSENSITIVE);
    static {
        Runnable runnable = () -> {
            for (Iterator<Map.Entry<JobInfo, Future>> it = list.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<JobInfo, Future> entry = it.next();
                if (entry.getValue().isDone() || entry.getValue().isCancelled()) it.remove();
            }
        };
        Pools.SCHEDULED.scheduleAtFixedRate(runnable,10,10,TimeUnit.SECONDS);
    }

    @Procedure
    @Description("apoc.periodic.list - list all jobs")
    public Stream<JobInfo> list() {
        return list.entrySet().stream().map( (e) -> e.getKey().update(e.getValue()));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.commit(statement,params) - runs the given statement in separate transactions until it returns 0")
    public Stream<RundownResult> commit(@Name("statement") String statement, @Name(value = "params", defaultValue = "") Map<String,Object> parameters) throws ExecutionException, InterruptedException {
        validateQuery(statement);
        Map<String,Object> params = parameters == null ? Collections.emptyMap() : parameters;
        long total = 0, executions = 0, updates = 0;
        long start = nanoTime();

        if (!LIMIT_PATTERN.matcher(statement).find()) {
            throw new IllegalArgumentException("the statement sent to apoc.periodic.commit must contain a `limit`");
        }

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
        } while (updates > 0 && !Util.transactionIsTerminated(terminationGuard));
        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(nanoTime() - start);
        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
        return Stream.of(new RundownResult(total,executions, timeTaken, batches.get(),failedBatches.get(),batchErrors, failedCommits.get(), commitErrors, wasTerminated));
    }

    private void recordError(Map<String, Long> executionErrors, Exception e) {
        executionErrors.compute(getMessages(e),(s, i) -> i == null ? 1 : i + 1);
    }

    private String getMessages(Throwable e) {
        Set<String> errors = new LinkedHashSet<>();
        do {
            errors.add(e.getMessage());
            e = e.getCause();
        } while (e.getCause() != null && !e.getCause().equals(e));
        return String.join("\n",errors);
    }

    public static class RundownResult {
        public final long updates;
        public final long executions;
        public final long runtime;
        public final long batches;
        public final long failedBatches;
        public final Map<String, Long> batchErrors;
        public final long failedCommits;
        public final Map<String, Long> commitErrors;
        public final boolean wasTerminated;

        public RundownResult(long total, long executions, long timeTaken, long batches, long failedBatches, Map<String, Long> batchErrors, long failedCommits, Map<String, Long> commitErrors, boolean wasTerminated) {
            this.updates = total;
            this.executions = executions;
            this.runtime = timeTaken;
            this.batches = batches;
            this.failedBatches = failedBatches;
            this.batchErrors = batchErrors;
            this.failedCommits = failedCommits;
            this.commitErrors = commitErrors;
            this.wasTerminated = wasTerminated;
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
            future.cancel(false);
            return Stream.of(info.update(future));
        }
        return Stream.empty();
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.submit('name',statement) - submit a one-off background statement")
    public Stream<JobInfo> submit(@Name("name") String name, @Name("statement") String statement) {
        validateQuery(statement);
        JobInfo info = submit(name, () -> {
            try {
                Iterators.count(db.execute(statement));
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }, log);
        return Stream.of(info);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.repeat('name',statement,repeat-rate-in-seconds, config) submit a repeatedly-called background statement. Fourth parameter 'config' is optional and can contain 'params' entry for nested statement.")
    public Stream<JobInfo> repeat(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate, @Name(value = "config", defaultValue = "{}") Map<String,Object> config ) {
        validateQuery(statement);
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        JobInfo info = schedule(name, () -> Iterators.count(db.execute(statement, params)),0,rate);
        return Stream.of(info);
    }

    private void validateQuery(String statement) {
        db.execute("EXPLAIN " + statement).close();
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.countdown('name',statement,repeat-rate-in-seconds) submit a repeatedly-called background statement until it returns 0")
    public Stream<JobInfo> countdown(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate) {
        validateQuery(statement);
        JobInfo info = submit(name, new Countdown(name, statement, rate, log), log);
        info.rate = rate;
        return Stream.of(info);
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public static <T> JobInfo submit(String name, Runnable task, Log log) {
        JobInfo info = new JobInfo(name);
        Future<T> future = list.remove(info);
        if (future != null && !future.isDone()) future.cancel(false);

        Runnable wrappingTask = wrapTask(name, task, log);
        Future newFuture = Pools.SCHEDULED.submit(wrappingTask);
        list.put(info,newFuture);
        return info;
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    private JobInfo schedule(String name, Runnable task, long delay, long repeat) {
        JobInfo info = new JobInfo(name,delay,repeat);
        Future future = list.remove(info);
        if (future != null && !future.isDone()) future.cancel(false);
        Runnable wrappingTask = wrapTask(name, task, log);
        ScheduledFuture<?> newFuture = Pools.SCHEDULED.scheduleWithFixedDelay(wrappingTask, delay, repeat, TimeUnit.SECONDS);
        list.put(info,newFuture);
        return info;
    }

    private static Runnable wrapTask(String name, Runnable task, Log log) {
        return () -> {
                log.debug("Executing task " + name);
                try {
                    task.run();
                } catch (Exception e) {
                    log.error("Error while executing task " + name + " because of the following exception (the task will be killed):", e);
                    throw e;
                }
                log.debug("Executed task " + name);
            };
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
        Map<String, String> fieldStatement = Util.map(
                "cypherLoop", cypherLoop,
                "cypherIterate", cypherIterate);
        validateQueries(fieldStatement);
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
                    iterateAndExecuteBatchedInSeparateThread((int) batchSize, false, false,0, result, params -> db.execute(cypherAction, params), 50, -1);
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

    /**
     * invoke cypherAction in batched transactions being feeded from cypherIteration running in main thread
     * @param cypherIterate
     * @param cypherAction
     */
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.periodic.iterate('statement returning items', 'statement per item', {batchSize:1000,iterateList:true,parallel:false,params:{},concurrency:50,retries:0}) YIELD batches, total - run the second statement for each item returned by the first statement. Returns number of batches and total processed rows")
    public Stream<BatchAndTotalResult> iterate(
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("config") Map<String,Object> config) {
        validateQuery(cypherIterate);

        long batchSize = Util.toLong(config.getOrDefault("batchSize", 10000));
        int concurrency = Util.toInteger(config.getOrDefault("concurrency", 50));
        boolean parallel = Util.toBoolean(config.getOrDefault("parallel", false));
        boolean iterateList = Util.toBoolean(config.getOrDefault("iterateList", true));
        long retries = Util.toLong(config.getOrDefault("retries", 0)); // todo sleep/delay or push to end of batch to try again or immediate ?
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        int failedParams = Util.toInteger(config.getOrDefault("failedParams", -1));
        try (Result result = db.execute(slottedRuntime(cypherIterate),params)) {
            Pair<String,Boolean> prepared = prepareInnerStatement(cypherAction, iterateList, result.columns(), "_batch");
            String innerStatement = prepared.first();
            iterateList=prepared.other();
            log.info("starting batching from `%s` operation using iteration `%s` in separate thread", cypherIterate,cypherAction);
            return iterateAndExecuteBatchedInSeparateThread((int)batchSize, parallel, iterateList, retries, result, (p) -> db.execute(innerStatement, merge(params, p)).close(), concurrency, failedParams);
        }
    }

    static String slottedRuntime(String cypherIterate) {
        if (RUNTIME_PATTERN.matcher(cypherIterate).find()) {
            return cypherIterate;
        }
        Matcher matcher = CYPHER_PREFIX_PATTERN.matcher(cypherIterate.substring(0, Math.min(15,cypherIterate.length())));
        return matcher.find() ? CYPHER_PREFIX_PATTERN.matcher(cypherIterate).replaceFirst(CYPHER_RUNTIME_SLOTTED) : CYPHER_RUNTIME_SLOTTED + cypherIterate;
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


    public Pair<String,Boolean> prepareInnerStatement(String cypherAction, boolean iterateList, List<String> columns, String iterator) {
        String names = columns.stream().map(Util::quote).collect(Collectors.joining("|"));
        boolean withCheck = regNoCaseMultiLine("[{$](" + names + ")\\}?\\s+AS\\s+").matcher(cypherAction).find();
        if (withCheck) return Pair.of(cypherAction, false);
        if (iterateList) {
            if (regNoCaseMultiLine("UNWIND\\s+[{$]" + iterator+"\\}?\\s+AS\\s+").matcher(cypherAction).find()) return Pair.of(cypherAction, true);
            String with = Util.withMapping(columns.stream(), (c) -> Util.quote(iterator) + "." + Util.quote(c) + " AS " + Util.quote(c));
            return Pair.of("UNWIND "+ Util.param(iterator)+" AS "+ Util.quote(iterator) + with + " " + cypherAction,true);
        }
        return Pair.of(Util.withMapping(columns.stream(), (c) ->  Util.param(c) + " AS " + Util.quote(c)) + cypherAction,false);
    }

    public Pattern regNoCaseMultiLine(String pattern) {
        return Pattern.compile(pattern,Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL);
    }


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
        try (Result result = db.execute(cypherIterate)) {
            return iterateAndExecuteBatchedInSeparateThread((int)batchSize, false, false, 0, result, p -> db.execute(cypherAction, p).close(), 50, -1);
        }
    }

    private Stream<BatchAndTotalResult> iterateAndExecuteBatchedInSeparateThread(int batchsize, boolean parallel, boolean iterateList, long retries,
                                                                                 Iterator<Map<String, Object>> iterator, Consumer<Map<String, Object>> consumer, int concurrency, int failedParams) {
        ExecutorService pool = parallel ? Pools.DEFAULT : Pools.SINGLE;
        List<Future<Long>> futures = new ArrayList<>(concurrency);
        long batches = 0;
        long start = System.nanoTime();
        AtomicLong count = new AtomicLong();
        AtomicInteger failedOps = new AtomicInteger();
        AtomicLong retried = new AtomicLong();
        Map<String,Long> operationErrors = new ConcurrentHashMap<>();
        AtomicInteger failedBatches = new AtomicInteger();
        Map<String,Long> batchErrors = new HashMap<>();
        Map<String, List<Map<String,Object>>> failedParamsMap = new ConcurrentHashMap<>();
        long successes = 0;
        do {
            if (Util.transactionIsTerminated(terminationGuard)) break;
            if (log.isDebugEnabled()) log.debug("execute in batch no " + batches + " batch size " + batchsize);
            List<Map<String,Object>> batch = Util.take(iterator, batchsize);
            long currentBatchSize = batch.size();
            Callable<Long> task;
            if (iterateList) {
                long finalBatches = batches;
                task = () -> {
                    long c = count.addAndGet(currentBatchSize);
                    if (Util.transactionIsTerminated(terminationGuard)) return 0L;
                    try {
                        Map<String, Object> params = Util.map("_count", c, "_batch", batch);
                        retried.addAndGet(retry(consumer,params,0,retries));
                    } catch (Exception e) {
                        failedOps.addAndGet(batchsize);
                        if (failedParams >= 0) {
                            failedParamsMap.put(Long.toString(finalBatches), new ArrayList<Map<String,Object>>(batch.subList(0, Math.min(failedParams+1, batch.size()))));
                        }
                        recordError(operationErrors, e);
                    }
                    return currentBatchSize;
                };
            } else {
                final long finalBatches = batches;
                task = () -> {
                    if (Util.transactionIsTerminated(terminationGuard)) return 0L;
                    return batch.stream().map(
                        p -> {
                            long c = count.incrementAndGet();
                            if (c % 1000 == 0 && Util.transactionIsTerminated(terminationGuard)) return 0;
                            try {
                                Map<String, Object> params = merge(p, Util.map("_count", c, "_batch", batch));
                                retried.addAndGet(retry(consumer,params,0,retries));
                            } catch (Exception e) {
                                failedOps.incrementAndGet();
                                if (failedParams >= 0) {
                                    failedParamsMap.put(Long.toString(finalBatches), new ArrayList<Map<String,Object>>(batch.subList(0, Math.min(failedParams+1, batch.size()))));
                                }
                                recordError(operationErrors, e);
                            }
                            return 1;
                        }).mapToLong(l -> l).sum();
                };
            }
            futures.add(Util.inTxFuture(pool, db, task));
            batches++;
            if (futures.size() > concurrency) {
                while (futures.stream().noneMatch(Future::isDone)) { // none done yet, block for a bit
                    LockSupport.parkNanos(1000);
                }
                Iterator<Future<Long>> it = futures.iterator();
                while (it.hasNext()) {
                    Future<Long> future = it.next();
                    if (future.isDone()) {
                        successes += Util.getFuture(future, batchErrors, failedBatches, 0L);
                        it.remove();
                    }
                }
            }
        } while (iterator.hasNext());
        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
        if (wasTerminated) {
            successes += futures.stream().mapToLong(f -> Util.getFutureOrCancel(f, batchErrors, failedBatches, 0L)).sum();
        } else {
            successes += futures.stream().mapToLong(f -> Util.getFuture(f, batchErrors, failedBatches, 0L)).sum();
        }
        Util.logErrors("Error during iterate.commit:", batchErrors, log);
        Util.logErrors("Error during iterate.execute:", operationErrors, log);
        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        BatchAndTotalResult result =
                new BatchAndTotalResult(batches, count.get(), timeTaken, successes, failedOps.get(), failedBatches.get(), retried.get(), operationErrors, batchErrors, wasTerminated, failedParamsMap);
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
        public final boolean wasTerminated;
        public final Map<String, List<Map<String,Object>>> failedParams;

        public BatchAndTotalResult(long batches, long total, long timeTaken, long committedOperations,
                                   long failedOperations, long failedBatches, long retries,
                                   Map<String, Long> operationErrors, Map<String, Long> batchErrors, boolean wasTerminated, Map<String, List<Map<String, Object>>> failedParams) {
            this.batches = batches;
            this.total = total;
            this.timeTaken = timeTaken;
            this.committedOperations = committedOperations;
            this.failedOperations = failedOperations;
            this.failedBatches = failedBatches;
            this.retries = retries;
            this.errorMessages = operationErrors;
            this.wasTerminated = wasTerminated;
            this.failedParams = failedParams;
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
        private transient final Log log;

        public Countdown(String name, String statement, long rate, Log log) {
            this.name = name;
            this.statement = statement;
            this.rate = rate;
            this.log = log;
        }

        @Override
        public void run() {
            if (Periodic.this.executeNumericResultStatement(statement, Collections.emptyMap()) > 0) {
                Pools.SCHEDULED.schedule(() -> submit(name, this, log), rate, TimeUnit.SECONDS);
            }
        }
    }
}
