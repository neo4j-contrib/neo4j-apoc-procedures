package apoc.cypher;

import apoc.ApocConfig;
import apoc.util.Util;
import apoc.version.Version;
import org.apache.commons.configuration2.Configuration;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Collection;
import java.util.Collections;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
    private boolean checkedVersion = false;

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

    @Override
    public void available() {

        // run initializers in a new thread
        // we need to wait until apoc procs are registered
        // unfortunately an AvailabilityListener is triggered before that
        Util.newDaemonThread(() -> {
            try {
                final boolean isSystemDatabase = db.databaseName().equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
                if (!isSystemDatabase) {
                    awaitApocProceduresRegistered();
                }

                if (defaultDb.equals(db.databaseName())) {
                    try {
                        final String apocFullVersion = Version.class.getPackage().getImplementationVersion();
                        final String apocVersion = getMajorMinVersion(apocFullVersion);
                        final List<String> versions = db.executeTransactionally("CALL dbms.components", Collections.emptyMap(),
                                r -> (List<String>) r.next().get("versions"));
                        final boolean versionDifferent = versions.stream().noneMatch(kernelVersion -> getMajorMinVersion(kernelVersion).equals(apocVersion));
                        if (versionDifferent) {
                            userLog.warn("The apoc version (%s) and the Neo4j DBMS versions %s are incompatible. \n" +
                                            "See the compatibility matrix in https://neo4j.com/labs/apoc/4.4/installation/ to see the correct version",
                                    apocFullVersion, versions.toString());
                        }
                    } catch (Exception ignored) {
                        // with embedded testdb, "call dbms.components" is not recognized here 
                    }
                }
                Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();

                for (String query : collectInitializers(isSystemDatabase, config)) {
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

    private String getMajorMinVersion(String completeVersion) {
        if (completeVersion == null) {
            return null;
        }
        final String[] split = completeVersion.split("\\.");
        return split[0] + "." + split[1];
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
        while (!areApocProceduresRegistered()) {
            Util.sleep(100);
        }
    }

    private boolean areApocProceduresRegistered() {
        try {
            return procs.getAllProcedures().stream().anyMatch(signature -> signature.name().toString().startsWith("apoc"));
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
