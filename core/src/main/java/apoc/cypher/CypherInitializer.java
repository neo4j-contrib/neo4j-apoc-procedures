package apoc.cypher;

import apoc.ApocConfig;
import apoc.util.Util;
import apoc.version.Version;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.SystemGraphComponent.Status;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Collection;

import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;


public class CypherInitializer implements AvailabilityListener {

    private final GraphDatabaseAPI db;
    private final Log userLog;
    private final GlobalProcedures procs;
    private final DependencyResolver dependencyResolver;
    private final String defaultDb;

    /**
     * indicates the status of the initializer, to be used for tests to ensure initializer operations are already done
     */
    private boolean finished = false;

    public CypherInitializer(GraphDatabaseAPI db, Log userLog) {
        this.db = db;
        this.userLog = userLog;
        this.dependencyResolver = db.getDependencyResolver();
        this.procs = dependencyResolver.resolveDependency(GlobalProcedures.class);
        this.defaultDb = dependencyResolver.resolveDependency(Config.class).get(GraphDatabaseSettings.default_database);
    }

    public boolean isFinished() {
        return finished;
    }

    public GraphDatabaseAPI getDb() {
        return db;
    }

    private void awaitUntil(BooleanSupplier supplier, int maxTime) {
        int time = 0;
        int sleep = 100;

        while (!supplier.getAsBoolean() && time < maxTime) {
            Util.sleep(sleep);
            time += sleep;
        }
    }

    @Override
    public void available() {

        // run initializers in a new thread
        // we need to wait until apoc procs are registered
        // unfortunately an AvailabilityListener is triggered before that
        Util.newDaemonThread(() -> {
            try {
                final boolean isSystemDatabase = db.databaseName().equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
                Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();
                var initializers = collectInitializers(isSystemDatabase, config);
                if (!isSystemDatabase) {
                    awaitApocProceduresRegistered();
                } else {
                    if (!initializers.isEmpty())
                    {
                        /* Await 1 min at max for the system database to show in
                           status CURRENT or REQUIRES_UPGRADE.
                           We do not do anything for the other possible status because
                           the dbms would not even be able to operate normally in them
                           (i.e. UNSUPPORTED_BUT_CAN_UPGRADE, UNSUPPORTED, UNSUPPORTED_FUTURE).

                           The reason for that is that the first time we initialize
                           it the built-in roles (and possibly other system databases
                           structures) will not be populated immediately in 4.4.
                           I.e. system.isAvailable can output true but some of the
                           Lifecycles may not have been run yet (UNINITIALIZED status)
                         */
                        awaitUntil( () ->
                                    {
                                        var components = dependencyResolver.resolveDependency(SystemGraphComponents.class);
                                        var status = components.detect(db);
                                        return (status == Status.CURRENT || status == Status.REQUIRES_UPGRADE);
                                    },  60000);
                    }
                }

                if (defaultDb.equals(db.databaseName())) {
                    try {
                       String neo4jVersion = org.neo4j.kernel.internal.Version.getNeo4jVersion();
                        final String apocFullVersion = Version.class.getPackage().getImplementationVersion();
                        if (isVersionDifferent(neo4jVersion, apocFullVersion)) {
                            userLog.warn("The apoc version (%s) and the Neo4j DBMS versions %s are incompatible. \n" +
                                            "See the compatibility matrix in https://neo4j.com/labs/apoc/4.4/installation/ to see the correct version",
                                    apocFullVersion, neo4jVersion);
                        }
                    } catch (Exception ignored) {
                        userLog.info("Cannot check APOC version compatibility because of a transient error. Retrying your request at a later time may succeed");
                    }
                }

                for (String query : initializers) {
                    try {
                        // we need to apply a retry strategy here since in systemdb we potentially conflict with
                        // creating constraints which could cause our query to fail with a transient error.
                        Util.retryInTx(userLog, db, tx -> Iterators.count(tx.execute(query)), 0, 5, retries -> { });
                        userLog.info("successfully initialized: " + query);
                    } catch (Exception e) {
                        userLog.error("error upon initialization, running: " + query, e);
                    }
                }
            } finally {
                finished = true;
            }
        }).start();
    }

    // the visibility is public only for testing purpose, it could be private otherwise
    public static boolean isVersionDifferent(String neo4jVersion, String apocVersion) {
        final String[] apocSplit = splitVersion(apocVersion);
        final String[] neo4jSplit = splitVersion(neo4jVersion);

        return !(apocSplit != null && neo4jSplit != null
                && apocSplit[0].equals(neo4jSplit[0])
                && apocSplit[1].equals(neo4jSplit[1]));
    }

    private static String[] splitVersion(String completeVersion) {
        if (StringUtils.isBlank(completeVersion)) {
            return null;
        }
        return completeVersion.split("[^\\d]");
    }

    private Collection<String> collectInitializers(boolean isSystemDatabase, Configuration config) {
        Map<String, String> initializers = new TreeMap<>();

        config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER + "." + db.databaseName())
                .forEachRemaining(key -> putIfNotBlank(initializers, key, config.getString(key)));

        // add legacy style initializers
        if (!isSystemDatabase) {
            config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER_CYPHER)
                    .forEachRemaining(key -> initializers.put(key, config.getString(key)));
        }

        return initializers.values();
    }

    private void putIfNotBlank(Map<String,String> map, String key, String value) {
        if ((value!=null) && (!value.isBlank())) {
            map.put(key, value);
        }
    }

    private void awaitApocProceduresRegistered() {
        while (!areProceduresRegistered("apoc")) {
            Util.sleep(100);
        }
    }

    private boolean areProceduresRegistered(String procStart) {
        try {
            return procs.getAllProcedures().stream().anyMatch(signature -> signature.name().toString().startsWith(procStart));
        } catch (ConcurrentModificationException e) {
            // if a CME happens (possible during procedure scanning)
            // we return false and the caller will try again
            return false;
        }
    }

    @Override
    public void unavailable() {
        // intentionally empty
    }
}
