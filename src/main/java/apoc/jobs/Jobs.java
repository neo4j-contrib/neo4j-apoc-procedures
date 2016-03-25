package apoc.jobs;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class Jobs {

    final static ScheduledExecutorService jobs = Executors.newScheduledThreadPool(Math.max(1,Runtime.getRuntime().availableProcessors()/4));

    @Context public GraphDatabaseAPI db;

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

    @Procedure
    public Stream<JobInfo> list() {
        return list.entrySet().stream().map( (e) -> e.getKey().update(e.getValue()));
    }

    @Procedure
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
    public Stream<JobInfo> submit(@Name("name") String name, @Name("statement") String statement) {
        JobInfo info = submit(name, () ->  Iterators.count(db.execute(statement)) );
        return Stream.of(info);
    }

    @Procedure
    public Stream<JobInfo> repeat(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate) {
        JobInfo info = schedule(name, () -> Iterators.count(db.execute(statement)),0,rate);
        return Stream.of(info);
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public static <T> JobInfo submit(String name, Runnable task) {
        JobInfo info = new JobInfo(name);
        Future<T> future = list.remove(info);
        if (future != null) future.cancel(false);

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
        if (future != null) future.cancel(false);

        ScheduledFuture<?> newFuture = jobs.scheduleWithFixedDelay(task, delay, repeat, TimeUnit.SECONDS);
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
}
