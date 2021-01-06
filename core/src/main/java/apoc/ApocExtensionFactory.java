package apoc;

import apoc.cypher.CypherInitializer;
import apoc.trigger.TriggerHandler;
import apoc.util.ApocUrlStreamHandlerFactory;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * @author mh
 * @since 14.05.16
 */
public class ApocExtensionFactory extends ExtensionFactory<ApocExtensionFactory.Dependencies> {

    static {
        try {
            URL.setURLStreamHandlerFactory(new ApocUrlStreamHandlerFactory());
        } catch (Error e) {
            System.err.println("APOC couln't set a URLStreamHandlerFactory since some other tool already did this (e.g. tomcat). This means you cannot use s3:// or hdfs:// style URLs in APOC. This is caused by a limitation of the JVM which we cannot fix. ");
        }
    }
    public ApocExtensionFactory() {
        super(ExtensionType.DATABASE, "APOC");
    }

    public interface Dependencies {
        GraphDatabaseAPI graphdatabaseAPI();
        JobScheduler scheduler();
        Procedures procedures();
        LogService log();
        AvailabilityGuard availabilityGuard();
        DatabaseManagementService databaseManagementService();
        ApocConfig apocConfig();
        TTLConfig ttlConfig();
        GlobalProcedures globalProceduresRegistry();
        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle();
        Pools pools();
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        LogService log = dependencies.log();
        return new ApocLifecycle(log, db, dependencies);
    }

    public static class ApocLifecycle extends LifecycleAdapter {

        private final LogService log;
        private final Log userLog;
        private final GraphDatabaseAPI db;
        private final Dependencies dependencies;
        private final Map<String, Lifecycle> services = new HashMap<>();

        // maps a component class to database name to resolver
        private final Map<Class, Map<String, Object>> resolvers = new ConcurrentHashMap<>();
        private final Collection<ApocGlobalComponents> apocGlobalComponents;


        public ApocLifecycle(LogService log, GraphDatabaseAPI db, Dependencies dependencies) {
            this.log = log;
            this.db = db;
            this.dependencies = dependencies;
            this.userLog = log.getUserLog(ApocExtensionFactory.class);
            this.apocGlobalComponents = Services.loadAll(ApocGlobalComponents.class);
        }

        public static void withNonSystemDatabase(GraphDatabaseService db, Consumer<Void> consumer) {
            if (!SYSTEM_DATABASE_NAME.equals(db.databaseName())) {
                consumer.accept(null);
            }
        }

        @Override
        public void init() throws Exception {
            withNonSystemDatabase(db, aVoid -> {
                for (ApocGlobalComponents c: apocGlobalComponents) {
                    services.putAll(c.getServices(db, dependencies));
                }

                String databaseName = db.databaseName();
                services.values().forEach(lifecycle -> dependencies.registerComponentLifecycle().addResolver(
                        databaseName,
                        lifecycle.getClass(),
                        lifecycle));

            });
        }

        @Override
        public void start() {
            withNonSystemDatabase(db, aVoid -> {
                services.forEach((key, value) -> {
                    try {
                        value.start();
                    } catch (Exception e) {
                        userLog.error("failed to start service " + key, e);
                    }
                });

            });

            AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
            for (ApocGlobalComponents c: apocGlobalComponents) {
                for (AvailabilityListener listener: c.getListeners(db, dependencies)) {
                    availabilityGuard.addListener(listener);
                }
            }
        }

        @Override
        public void stop() {
            withNonSystemDatabase(db, aVoid -> {
                services.forEach((key, value) -> {
                    try {
                        value.stop();
                    } catch (Exception e) {
                        userLog.error("failed to stop service " + key, e);
                    }
                });
            });
        }
    }
}
