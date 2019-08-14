package apoc;

import apoc.custom.CypherProcedures;
import apoc.cypher.CypherInitializer;
import apoc.trigger.Trigger;
import apoc.ttl.TTLLifeCycle;
import apoc.util.ApocUrlStreamHandlerFactory;
import apoc.uuid.Uuid;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
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
import org.neo4j.scheduler.JobScheduler;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

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
        Config config();
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
        private CypherProcedures.CustomProcedureStorage customProcedureStorage;

        private final Map<String, Lifecycle> services = new HashMap<>();

        public ApocLifecycle(LogService log, GraphDatabaseAPI db, Dependencies dependencies) {
            this.log = log;
            this.db = db;
            this.dependencies = dependencies;
            userLog = log.getUserLog(ApocExtensionFactory.class);
        }

        @Override
        public void start() {
            ApocConfig.withNonSystemDatabase(db, aVoid -> {

                ApocConfiguration.initialize(db);
//                Pools.NEO4J_SCHEDULER = dependencies.scheduler();

                services.put("ttl", new TTLLifeCycle(dependencies.scheduler(), db, dependencies.config(), log.getUserLog(TTLLifeCycle.class)));
                services.put("uuid", new Uuid.UuidLifeCycle(db, dependencies.databaseManagementService(), log.getUserLog(Uuid.class)));
                services.put("trigger", new Trigger.LifeCycle(db, dependencies.databaseManagementService(), log.getUserLog(Trigger.class)));
                services.put("pools", new PoolsLifecycle());

                services.entrySet().stream().forEach(entry -> {
                    try {
                        entry.getValue().start();
                    } catch (Exception e) {
                        userLog.error("failed to start service " + entry.getKey(), e);
                    }
                });

                customProcedureStorage = new CypherProcedures.CustomProcedureStorage(db, log.getUserLog(CypherProcedures.class));
                AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
                availabilityGuard.addListener(customProcedureStorage);
                availabilityGuard.addListener(new CypherInitializer(db, log.getUserLog(CypherInitializer.class)));

            });
        }

        @Override
        public void stop() {
            ApocConfig.withNonSystemDatabase(db, aVoid -> {
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
