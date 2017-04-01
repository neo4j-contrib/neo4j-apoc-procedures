package apoc;

import apoc.index.IndexUpdateTransactionEventHandler;
import apoc.schema.AssertSchemaProcedure;
import apoc.trigger.Trigger;
import apoc.ttl.TTLLifeCycle;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

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
        return new ApocLifecycle(log, db, dependencies);
    }

    public static class ApocLifecycle extends LifecycleAdapter {

        private final LogService log;
        private final GraphDatabaseAPI db;
        private final Dependencies dependencies;
        private Trigger.LifeCycle triggerLifeCycle;
        private Log userLog;
        private TTLLifeCycle ttlLifeCycle;

        private IndexUpdateTransactionEventHandler.LifeCycle indexUpdateLifeCycle;

        public ApocLifecycle(LogService log, GraphDatabaseAPI db, Dependencies dependencies) {
            this.log = log;
            this.db = db;
            this.dependencies = dependencies;
            userLog = log.getUserLog(ApocKernelExtensionFactory.class);
        }

        public IndexUpdateTransactionEventHandler.LifeCycle getIndexUpdateLifeCycle() {
            return indexUpdateLifeCycle;
        }

        @Override
        public void start() throws Throwable {
            ApocConfiguration.initialize(db);
            Pools.NEO4J_SCHEDULER = dependencies.scheduler();
            registerCustomProcedures();
            ttlLifeCycle = new TTLLifeCycle(Pools.NEO4J_SCHEDULER, db, log.getUserLog(TTLLifeCycle.class));
            ttlLifeCycle.start();

            triggerLifeCycle = new Trigger.LifeCycle(db, log.getUserLog(Trigger.class));
            triggerLifeCycle.start();
            indexUpdateLifeCycle = new IndexUpdateTransactionEventHandler.LifeCycle(db, log.getUserLog(Procedures.class));
            indexUpdateLifeCycle.start();
        }

        public void registerCustomProcedures() {
            try {
                dependencies.procedures().register(new AssertSchemaProcedure(db, log.getUserLog(AssertSchemaProcedure.class)));
            } catch(Exception|Error e) {
                log.getUserLog(ArithmeticException.class).error("Cannot register procedure AssertSchemaProcedure",e);
            }
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
            if (indexUpdateLifeCycle !=null)
                try {
                    indexUpdateLifeCycle.stop();
                } catch(Exception e) {
                    userLog.warn("Error stopping index update service",e);
                }
        }

    }
}
