package apoc;

import apoc.schema.AssertSchemaProcedure;
import apoc.util.Util;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.ApocGroup;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.concurrent.TimeUnit;

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
//        Log log();
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) throws Throwable {
        GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        Log log = null; // dependencies.log();
        return new LifecycleAdapter() {

            public JobScheduler.JobHandle ttlIndexJobHandle;
            private JobScheduler.JobHandle ttlJobHandle;

            @Override
            public void start() throws Throwable {
                ApocConfiguration.initialize(db);
                dependencies.procedures().register(new AssertSchemaProcedure(db, log));

                Object enabled = ApocConfiguration.get("ttl.enabled", null);
                if (Util.toBoolean(enabled)) {
                    ttlIndexJobHandle = dependencies.scheduler().schedule(ApocGroup.TTL_GROUP,
                            () -> {
                                db.execute("CREATE INDEX ON :TTL(ttl)");
                            }, 30, TimeUnit.SECONDS);

                    long ttlSchedule = Util.toLong(ApocConfiguration.get("ttl.schedule", 60));
                    ttlJobHandle = dependencies.scheduler().scheduleRecurring(ApocGroup.TTL_GROUP,
                            () -> {
                                db.execute("MATCH (t:TTL) where t.ttl < timestamp() WITH t LIMIT 1000 DETACH DELETE t");
                            },
                            ttlSchedule, TimeUnit.SECONDS);
                }
                Pools.NEO4J_SCHEDULER = dependencies.scheduler();
            }

            @Override
            public void stop() throws Throwable {
                if (ttlIndexJobHandle != null) ttlIndexJobHandle.cancel(true);
                if (ttlJobHandle != null) ttlJobHandle.cancel(true);
            }
        };
    }

}
