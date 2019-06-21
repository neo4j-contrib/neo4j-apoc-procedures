package apoc.cypher;

import apoc.ApocConfiguration;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class CypherInitializer implements AvailabilityListener {
    public static final String INITIALIZER_CYPHER = "initializer.cypher";

    private final GraphDatabaseAPI db;
    private final Log userLog;

    public CypherInitializer(GraphDatabaseAPI db, Log userLog) {
        this.db = db;
        this.userLog = userLog;
    }

    @Override
    public void available() {
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
    }

    @Override
    public void unavailable() {
        // intentionally empty
    }
}
