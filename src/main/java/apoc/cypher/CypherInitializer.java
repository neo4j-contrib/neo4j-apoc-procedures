package apoc.cypher;

import apoc.ApocConfiguration;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.*;

public class CypherInitializer implements AvailabilityListener {
    public static final String INITIALIZER_CYPHER = "initializer.cypher";

    private final GraphDatabaseAPI db;
    private final Log userLog;
    private final Procedures procs;

    /**
     * indicates the status of the initializer, to be used for tests to ensure initializer operations are already done
     */
    private boolean finished = false;

    public CypherInitializer(GraphDatabaseAPI db, Log userLog) {
        this.db = db;
        this.userLog = userLog;
        procs = db.getDependencyResolver().resolveDependency(Procedures.class, DependencyResolver.SelectionStrategy.FIRST);
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

                Map<String, Object> stringObjectMap;
                String singleInitializer = ApocConfiguration.get(INITIALIZER_CYPHER, null);
                if (singleInitializer != null) {
                    stringObjectMap = Collections.singletonMap("1", singleInitializer);
                } else {
                    stringObjectMap = ApocConfiguration.get(INITIALIZER_CYPHER);
                }

                SortedMap<String, Object> initializers = new TreeMap<>(stringObjectMap);
                for (Object initializer: initializers.values()) {
                    String query = initializer.toString();
                    try {
                        db.execute(query);
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
