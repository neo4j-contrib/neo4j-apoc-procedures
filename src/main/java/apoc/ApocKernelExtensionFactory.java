package apoc;

import apoc.index.IndexUpdateTransactionEventHandler;
import apoc.schema.AssertSchemaProcedure;
import apoc.trigger.Trigger;
import apoc.ttl.TTLLifeCycle;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author mh
 * @since 14.05.16
 */
public class ApocKernelExtensionFactory extends KernelExtensionFactory<ApocKernelExtensionFactory.Dependencies>{

    public ApocKernelExtensionFactory() {
        super("APOC");
    }

    public interface Dependencies {
        GraphDatabaseAPI graphdatabaseAPI();
        JobScheduler scheduler();
        Procedures procedures();
        LogService log();
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) throws Throwable {
        GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        LogService log = dependencies.log();
        return new LifecycleAdapter() {

            private Trigger.LifeCycle triggerLifeCycle;
            private Log userLog = log.getUserLog(ApocKernelExtensionFactory.class);
            private TTLLifeCycle ttlLifeCycle;
            private IndexUpdateTransactionEventHandler indexUpdateTransactionEventHandler;

            @Override
            public void start() throws Throwable {
                ApocConfiguration.initialize(db);
                installIndexUpdateTransactionEventHandler();
                Pools.NEO4J_SCHEDULER = dependencies.scheduler();
                dependencies.procedures().register(new AssertSchemaProcedure(db, log.getUserLog(AssertSchemaProcedure.class)));
                ttlLifeCycle = new TTLLifeCycle(Pools.NEO4J_SCHEDULER, db, log.getUserLog(TTLLifeCycle.class));
                ttlLifeCycle.start();
                triggerLifeCycle = new Trigger.LifeCycle(db, log.getUserLog(Trigger.class));
                triggerLifeCycle.start();
            }

            private void installIndexUpdateTransactionEventHandler() {
                boolean enabled = ApocConfiguration.isEnabled("autoIndex.enabled");
                if (enabled) {
                    boolean async = ApocConfiguration.isEnabled("autoIndex.async");
                    final Log userLog = log.getUserLog(Procedures.class);
                    indexUpdateTransactionEventHandler = new IndexUpdateTransactionEventHandler(db, userLog, async);
                    if (async) {
                        startIndexTrackingThread(db, indexUpdateTransactionEventHandler.getIndexCommandQueue(),
                                Long.parseLong(ApocConfiguration.get("autoIndex.async_rollover_opscount", "10000")),
                                Long.parseLong(ApocConfiguration.get("autoIndex.async_rollover_millis", "5000")),
                                userLog
                                );
                    }
                    db.registerTransactionEventHandler(indexUpdateTransactionEventHandler);
                }
            }

            private void startIndexTrackingThread(GraphDatabaseAPI db, BlockingQueue<Consumer<Void>> indexCommandQueue, long opsCountRollover, long millisRollover, Log log) {
                new Thread(() -> {
                    Transaction tx = db.beginTx();
                    int opsCount = 0;
                    long lastCommit = System.currentTimeMillis();
                    try {
                        while (true) {
                            Consumer<Void> indexCommand = indexCommandQueue.poll(millisRollover, TimeUnit.MILLISECONDS);
                            long now = System.currentTimeMillis();
                            if ((opsCount>0) && ((now - lastCommit > millisRollover) || (opsCount > opsCountRollover))) {
                                tx.success();
                                tx.close();
                                tx = db.beginTx();
                                lastCommit = now;
                                opsCount = 0;
                                log.info("background indexing thread doing tx rollover");
                            }
                            if (indexCommand == null) {
                                // check if a database shutdown is already in progress, if so, terminate this thread
                                boolean running = db.getDependencyResolver().resolveDependency(LifeSupport.class).isRunning();
                                if (!running) {
                                    log.info("system shutdown detected, terminating indexing background thread");
                                    break;
                                }
                            } else {
                                opsCount++;
                                indexCommand.accept(null);
                            }
                        }
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    } finally {
                        tx.success();
                        tx.close();
                        log.info("stopping background thread for async index updates");
                    }
                }).start();
                log.info("started background thread for async index updates");
            }


            @Override
            public void stop() throws Throwable {
                if (ttlLifeCycle !=null)
                    try {
                        ttlLifeCycle.stop();
                    } catch(Exception e) {
                        userLog.warn("Error stopping ttl service",e);
                    }
                if (triggerLifeCycle !=null)
                    try {
                        triggerLifeCycle.stop();
                    } catch(Exception e) {
                        userLog.warn("Error stopping trigger service",e);
                    }
                if (indexUpdateTransactionEventHandler!=null) {
                    db.unregisterTransactionEventHandler(indexUpdateTransactionEventHandler);
                }
            }
        };
    }

}
