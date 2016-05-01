package apoc.periodic;

import apoc.Description;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.lang.System.currentTimeMillis;

public class Periodic {

    final static ScheduledExecutorService jobs = Executors.newScheduledThreadPool(Math.max(1,Runtime.getRuntime().availableProcessors()/4));

    final static ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

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
        jobs.scheduleAtFixedRate(runnable,10,10,TimeUnit.SECONDS);
    }

    @Context
    public KernelTransaction tx;

    @Procedure
    @Description("apoc.periodic.list - list all jobs")
    public Stream<JobInfo> list() {
        return list.entrySet().stream().map( (e) -> e.getKey().update(e.getValue()));
    }

    @Procedure
    @PerformsWrites
    @Description("apoc.periodic.commit(statement,params) - runs the given statement in separate transactions until it returns 0")
    public Stream<RundownResult> commit(@Name("statement") String statement, @Name("params") Map<String,Object> parameters) throws ExecutionException, InterruptedException {
        Map<String,Object> params = parameters == null ? Collections.emptyMap() : parameters;
        long sum = 0, executions = 0, count;
        long start = currentTimeMillis();
        do {
            count = jobs.submit(() -> executeNumericResultStatement(statement, params)).get();
            sum += count;
            if (count>0) executions++;
        } while (count > 0);
        return Stream.of(new RundownResult(sum,executions, currentTimeMillis() - start));
    }

    public static class RundownResult {
        public final long updates;
        public final long executions;
        public final long runtime;

        public RundownResult(long updates, long executions, long runtime) {
            this.updates = updates;
            this.executions = executions;
            this.runtime = runtime;
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
    @Description("apoc.periodic.schedule('name',statement,repeat-time-in-seconds) submit a repeatedly-called background statement")
    public Stream<JobInfo> repeat(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate) {
        JobInfo info = schedule(name, () -> Iterators.count(db.execute(statement)),0,rate);
        return Stream.of(info);
    }

    // TODO
    @Description("apoc.periodic.countdown('name',statement,repeat-time-in-seconds) submit a repeatedly-called background statement until it returns 0")
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

        Future newFuture = jobs.submit(task);
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

        ScheduledFuture<?> newFuture = jobs.scheduleWithFixedDelay(task, delay, repeat, TimeUnit.SECONDS);
        list.put(info,newFuture);
        return info;
    }


    /**
     * invoke cypherAction in batched transactions being feeded from cypherIteration running in main thread
     * @param cypherIterate
     * @param cypherAction
     * @param batchSize
     */
    @Procedure
    @PerformsWrites
    @Description("apoc.periodic.rock_n_roll('some cypher for iteration', 'some cypher as action on each iteration', 10000) YIELD batches, total - run the action statement in batches over the iterator statement's results in a separate thread. Returns number of batches and total processed rows")
    public Stream<BatchAndTotalResult> rock_n_roll(
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("batchSize") long batchSize) {

        log.info("starting batched operation using iteration `%s` in separate thread", cypherIterate);
        try (Result result = db.execute(cypherIterate)) {
            return iterateAndExecuteBatchedInSeparateThread((int)batchSize, result, params -> {
                db.execute(cypherAction, params);
            });
        }
    }

    private <T> Stream<BatchAndTotalResult> iterateAndExecuteBatchedInSeparateThread(int batchsize, Iterator<T> iterator, Consumer<T> consumer) {
        final int[] opsCount = {0};
        final int[] batchesCount = {0};
        try {
            // create transaction in separate thread
            // using 1 element arrays to enable modification
            final Transaction[] workerTransaction = {singleThreadExecutor.submit(() -> db.beginTx()).get()};

            iterator.forEachRemaining(t -> singleThreadExecutor.submit(() -> {
                log.debug("iteration " + opsCount[0]);
                consumer.accept(t);
                if ((++opsCount[0]) % batchsize == 0) {
                    log.info("rolling over transaction at opsCount %d", opsCount[0]);
                    batchesCount[0]++;
                    workerTransaction[0].success();
                    workerTransaction[0].close();
                    workerTransaction[0] = db.beginTx();
                }
            }));
            singleThreadExecutor.submit(() -> {
                workerTransaction[0].success();
                workerTransaction[0].close();
                log.info("finalizing - closing transaction");
            }).get();

        } catch (InterruptedException | ExecutionException e) {
            log.error("exception happened", e);
            throw new RuntimeException(e);
        }
        return Collections.singletonList(new BatchAndTotalResult(batchesCount[0], opsCount[0])).stream();
    }

    public static class BatchAndTotalResult {
        public long batches;
        public long total;

        public BatchAndTotalResult(long batches, long total) {
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

        ScheduledFuture<?> newFuture = jobs.schedule(task, delay, TimeUnit.SECONDS);
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
                jobs.schedule(() -> submit(name, this), rate, TimeUnit.SECONDS);
            }
        }
    }
}
