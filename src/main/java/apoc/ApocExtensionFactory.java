package apoc;

import apoc.custom.CypherProcedures;
import apoc.custom.CypherProceduresHandler;
import apoc.cypher.CypherInitializer;
import apoc.trigger.Trigger;
import apoc.trigger.TriggerHandler;
import apoc.ttl.TTLLifeCycle;
import apoc.util.ApocUrlStreamHandlerFactory;
import apoc.uuid.Uuid;
import apoc.uuid.UuidHandler;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.scheduler.JobScheduler;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
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
        GlobalProceduresRegistry globalProceduresRegistry();
        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle();
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        LogService log = dependencies.log();
        return new ApocLifecycle(log, db, dependencies);
    }

    public static class ApocLifecycle extends LifecycleAdapter {

        private final LogService log;
        private final GraphDatabaseAPI db;
        private final Dependencies dependencies;
        private Log userLog;

        private final Map<String, Lifecycle> services = new HashMap<>();
        private CypherProceduresHandler cypherProceduresHandler;

        public ApocLifecycle(LogService log, GraphDatabaseAPI db, Dependencies dependencies) {
            this.log = log;
            this.db = db;
            this.dependencies = dependencies;
            userLog = log.getUserLog(ApocExtensionFactory.class);
        }

        public static void withNonSystemDatabase(GraphDatabaseService db, Consumer<Void> consumer) {
            if (!SYSTEM_DATABASE_NAME.equals(db.databaseName())) {
                consumer.accept(null);
            }
        }

        @Override
        public void init() throws Exception {
            withNonSystemDatabase(db, aVoid -> {

                services.put("ttl", new TTLLifeCycle(dependencies.scheduler(), db, dependencies.apocConfig(), log.getUserLog(TTLLifeCycle.class)));
                services.put("uuid", new UuidHandler(db,
                        dependencies.databaseManagementService(),
                        log.getUserLog(Uuid.class),
                        dependencies.apocConfig(),
                        dependencies.globalProceduresRegistry())
                );
                services.put("trigger", new TriggerHandler(db,
                        dependencies.databaseManagementService(),
                        dependencies.apocConfig(),
                        log.getUserLog(Trigger.class),
                        dependencies.globalProceduresRegistry())
                );

                RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle = dependencies.registerComponentLifecycle();
                String databaseNamme = db.databaseName();
                services.values().forEach(lifecycle -> registerComponentLifecycle.addResolver(
                        databaseNamme,
                        lifecycle.getClass(),
                        lifecycle));

                cypherProceduresHandler = new CypherProceduresHandler(
                        db,
                        dependencies.scheduler(),
                        dependencies.apocConfig(),
                        log.getUserLog(CypherProcedures.class),
                        dependencies.globalProceduresRegistry()
                );
                registerComponentLifecycle.addResolver(databaseNamme, CypherProceduresHandler.class, cypherProceduresHandler);
            });
        }

        @Override
        public void start() {
            withNonSystemDatabase(db, aVoid -> {
                services.entrySet().stream().forEach(entry -> {
                    try {
                        entry.getValue().start();
                    } catch (Exception e) {
                        userLog.error("failed to start service " + entry.getKey(), e);
                    }
                });

                AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
                availabilityGuard.addListener(cypherProceduresHandler);
                availabilityGuard.addListener(new CypherInitializer(db, log.getUserLog(CypherInitializer.class)));

            });
        }

        @Override
        public void stop() {
            withNonSystemDatabase(db, aVoid -> {
                services.entrySet().stream().forEach(entry -> {
                    try {
                        entry.getValue().stop();
                    } catch (Exception e) {
                        userLog.error("failed to stop service " + entry.getKey(), e);
                    }
                });
            });
        }

    }
}
