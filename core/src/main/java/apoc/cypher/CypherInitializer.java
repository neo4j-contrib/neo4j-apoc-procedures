package apoc.cypher;

import apoc.ApocConfig;
import apoc.util.Util;
import apoc.version.Version;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
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
    private volatile boolean finished = false;

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
                    final List<String> versions = db.executeTransactionally("CALL dbms.components", Collections.emptyMap(),
                            r -> (List<String>) r.next().get("versions"));
                    final String apocFullVersion = Version.class.getPackage().getImplementationVersion();
                    if (isVersionDifferent(versions, apocFullVersion)) {
                        userLog.warn("The apoc version (%s) and the Neo4j DBMS versions %s are incompatible. \n" +
                                        "See the compatibility matrix in https://neo4j.com/labs/apoc/4.4/installation/ to see the correct version",
                                apocFullVersion, versions.toString());
                    }
                }
                Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();

                for (String query : collectInitializers(config)) {
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
    public static boolean isVersionDifferent(List<String> versions, String apocVersion) {
        final String[] apocSplit = splitVersion(apocVersion);
        return versions.stream()
                .noneMatch(kernelVersion -> {
                    final String[] kernelSplit = splitVersion(kernelVersion);
                    return apocSplit != null && kernelSplit != null
                            && apocSplit[0].equals(kernelSplit[0])
                            && apocSplit[1].equals(kernelSplit[1]);
                });
    }

    private static String[] splitVersion(String completeVersion) {
        if (StringUtils.isBlank(completeVersion)) {
            return null;
        }
        return completeVersion.split("[^\\d]");
    }

    private Collection<String> collectInitializers(Configuration config) {
        Map<String, String> initializers = new TreeMap<>();

        config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER + "." + db.databaseName())
                .forEachRemaining(key -> putIfNotBlank(initializers, key, config.getString(key)));

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
