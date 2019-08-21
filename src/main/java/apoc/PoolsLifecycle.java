package apoc;

import apoc.periodic.Periodic;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;

public class PoolsLifecycle extends LifecycleAdapter {

    private static PoolsLifecycle theInstance = null;

    public final static int DEFAULT_SCHEDULED_THREADS = Runtime.getRuntime().availableProcessors() / 4;
    public final static int DEFAULT_POOL_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    private ExecutorService singleExecutorService = Executors.newSingleThreadExecutor();
    private ScheduledExecutorService scheduledExecutorService
            = Executors.newScheduledThreadPool(getNoThreadsInScheduledPool());
    private ExecutorService defaultExecutorService;

    private final Map<Periodic.JobInfo,Future> jobList = new ConcurrentHashMap<>();

//    public static JobScheduler NEO4J_SCHEDULER = null;

    public PoolsLifecycle() {
        if (theInstance!=null) {
            throw new IllegalStateException("you cannot instantiate Pools more than once");
        }
        theInstance = this;
    }

    @Override
    public void start() {

        int threads = getNoThreadsInDefaultPool();
        int queueSize = threads * 25;
        this.defaultExecutorService = new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new CallerBlocksPolicy());

        pools().getScheduledExecutorService().scheduleAtFixedRate(() -> {
            for (Iterator<Map.Entry<Periodic.JobInfo, Future>> it = jobList.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Periodic.JobInfo, Future> entry = it.next();
                if (entry.getValue().isDone() || entry.getValue().isCancelled()) it.remove();
            }
        },10,10,TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        Stream.of(singleExecutorService, defaultExecutorService, scheduledExecutorService).forEach( service -> {
            try {
                service.shutdown();
                service.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {

            }
        });
        theInstance = null;
    }

    public ExecutorService getSingleExecutorService() {
        return singleExecutorService;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public ExecutorService getDefaultExecutorService() {
        return defaultExecutorService;
    }

    public Map<Periodic.JobInfo, Future> getJobList() {
        return jobList;
    }

    public static PoolsLifecycle pools() {
        return theInstance;
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

    public int getNoThreadsInDefaultPool() {
        int maxThreads = apocConfig().getInt(ApocConfig.APOC_CONFIG_JOBS_POOL_NUM_THREADS, DEFAULT_POOL_THREADS);
        return Math.max(1, maxThreads);
    }
    public int getNoThreadsInScheduledPool() {
        Integer maxThreads = apocConfig().getInt(ApocConfig.APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS, DEFAULT_SCHEDULED_THREADS);
        return Math.max(1, maxThreads);
    }

    public <T> Future<Void> processBatch(List<T> batch, GraphDatabaseService db, Consumer<T> action) {
        return defaultExecutorService.submit((Callable<Void>) () -> {
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
