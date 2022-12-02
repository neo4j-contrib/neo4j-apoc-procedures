package apoc.trigger;

import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;

public class TriggerTestUtil {
    public static final long TIMEOUT = 10L;

    public static void awaitTriggerDiscovered(GraphDatabaseService db, String name, String query) {
        awaitTriggerDiscovered(db, name, query, false);
    }

    public static void awaitTriggerDiscovered(GraphDatabaseService db, String name, String query, boolean paused) {
        testCallEventually(db, "CALL apoc.trigger.list() YIELD name, query, paused WHERE name = $name RETURN query, paused",
                Map.of("name", name),
                row -> {
                    assertEquals(query, row.get("query"));
                    assertEquals(paused, row.get("paused"));
                }, TIMEOUT);
    }
}
