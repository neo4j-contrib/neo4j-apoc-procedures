package apoc.cypher;

import apoc.ApocConfig;
import org.apache.commons.configuration2.Configuration;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.ConcurrentModificationException;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CypherInitializer implements AvailabilityListener {

    public static final String CONFIG_APOC_INITIALIZER_CYPHER = "apoc.initializer.cypher";

    private final GraphDatabaseAPI db;
    private final Log userLog;
    private final GlobalProcedures procs;
    private final DependencyResolver dependencyResolver;

    /**
     * indicates the status of the initializer, to be used for tests to ensure initializer operations are already done
     */
    private boolean finished = false;

    public CypherInitializer(GraphDatabaseAPI db, Log userLog) {
        this.db = db;
        this.userLog = userLog;
        this.dependencyResolver = db.getDependencyResolver();
        this.procs = dependencyResolver.resolveDependency(GlobalProcedures.class);
    }

    public boolean isFinished() {
        return finished;
    }

    @Override
    public void available() {

        // run initializers in a new thread
        // we need to wait until apoc procs are registered
        // unfortunately an AvailabilityListener is triggered before that
        new Thread(() -> {

            try {
                awaitApocProceduresRegistered();
                Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();

                TreeMap<String, String> initializers = Iterators.stream(config.getKeys(CONFIG_APOC_INITIALIZER_CYPHER))
                        .collect(Collectors.toMap(k -> k, k -> config.getString(k),
                                (v1, v2) -> {
                                    throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));
                                },
                                TreeMap::new));

                for (Object initializer: initializers.values()) {
                    String query = initializer.toString();
                    try {
                        db.executeTransactionally(query);
                        userLog.info("successfully initialized: " + query);
                    } catch (Exception e) {
                        userLog.warn("error upon initialization, running: "+query, e);
                    }
                }
            } finally {
                finished = true;
            }
        }).start();
    }

    private void awaitApocProceduresRegistered() {
        while (!areApocProceduresRegistered()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
