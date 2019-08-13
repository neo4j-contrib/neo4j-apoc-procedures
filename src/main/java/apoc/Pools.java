package apoc;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.scheduler.JobScheduler;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static apoc.ApocConfig.apocConfig;

public class Pools {

    public final static int DEFAULT_SCHEDULED_THREADS = Runtime.getRuntime().availableProcessors() / 4;
    public final static int DEFAULT_POOL_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    public final static ExecutorService SINGLE = createSinglePool();
    public final static ExecutorService DEFAULT = createDefaultPool();
    public final static ScheduledExecutorService SCHEDULED = createScheduledPool();
    public static JobScheduler NEO4J_SCHEDULER = null;

    static {
        for (ExecutorService service : Arrays.asList(SINGLE, DEFAULT, SCHEDULED)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    service.shutdown();
                    service.awaitTermination(10,TimeUnit.SECONDS);
                } catch(Exception ignore) {
                    //
                }
            }));
        }
    }
    private Pools() {
        throw new UnsupportedOperationException();
    }

    public static ExecutorService createDefaultPool() {
        int threads = getNoThreadsInDefaultPool();
        int queueSize = threads * 25;
        return new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new CallerBlocksPolicy());
    }

    static class CallerBlocksPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            // Submit again by directly injecting the task into the work queue, waiting if necessary, but also periodically checking if the pool has been
            // shut down.
            FutureTask<Void> task = new FutureTask<>( r, null );
            BlockingQueue<Runnable> queue = executor.getQueue();
            while (!executor.isShutdown()) {
                try {
                    if ( queue.offer( task, 250, TimeUnit.MILLISECONDS ) )
                    {
                        while ( !executor.isShutdown() )
                        {
                            try
                            {
                                task.get( 250, TimeUnit.MILLISECONDS );
                                return; // Success!
                            }
                            catch ( TimeoutException ignore )
                            {
                                // This is fine an expected. We just want to check that the executor hasn't been shut down.
                            }
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static int getNoThreadsInDefaultPool() {
        int maxThreads = apocConfig().getInt(ApocConfig.APOC_CONFIG_JOBS_POOL_NUM_THREADS, DEFAULT_POOL_THREADS);
        return Math.max(1, maxThreads);
    }
    public static int getNoThreadsInScheduledPool() {
        Integer maxThreads = apocConfig().getInt(ApocConfig.APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS, DEFAULT_SCHEDULED_THREADS);
        return Math.max(1, maxThreads);
    }

    private static ExecutorService createSinglePool() {
        return Executors.newSingleThreadExecutor();
    }

    private static ScheduledExecutorService createScheduledPool() {
        return Executors.newScheduledThreadPool(getNoThreadsInScheduledPool());
    }

    public static <T> Future<Void> processBatch(List<T> batch, GraphDatabaseService db, Consumer<T> action) {
        return DEFAULT.submit((Callable<Void>) () -> {
                try (Transaction tx = db.beginTx()) {
                    batch.forEach(action);
                    tx.success();
                }
                return null;
            }
        );
    }

    public static <T> T force(Future<T> future) throws ExecutionException {
        while (true) {
            try {
                return future.get();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }
}
