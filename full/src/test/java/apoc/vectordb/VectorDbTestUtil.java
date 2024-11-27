package apoc.vectordb;

import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;

public class VectorDbTestUtil {

    public enum EntityType {
        NODE,
        REL,
        FALSE
    }

    public static void dropAndDeleteAll(GraphDatabaseService db) {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    public static void assertBerlinResult(Map row, EntityType entityType) {
        assertBerlinResult(row, "1", entityType);
    }

    public static void assertBerlinResult(Map row, String id, EntityType entityType) {
        assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
        assertEquals(id, row.get("id").toString());
        if (!entityType.equals(EntityType.FALSE)) {
            String entity = entityType.equals(EntityType.NODE) ? "node" : "rel";
            Map<String, Object> props = ((Entity) row.get(entity)).getAllProperties();
            assertBerlinProperties(props);
        }
    }

    public static void assertLondonResult(Map row, EntityType entityType) {
        assertLondonResult(row, "2", entityType);
    }

    public static void assertLondonResult(Map row, String id, EntityType entityType) {
        assertEquals(Map.of("city", "London", "foo", "two"), row.get("metadata"));
        assertEquals(id, row.get("id").toString());
        if (!entityType.equals(EntityType.FALSE)) {
            String entity = entityType.equals(EntityType.NODE) ? "node" : "rel";
            Map<String, Object> props = ((Entity) row.get(entity)).getAllProperties();
            assertLondonProperties(props);
        }
    }

    public static void assertNodesCreated(GraphDatabaseService db) {
        testResult(
                db,
                "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                VectorDbTestUtil::vectorEntityAssertions);
    }

    public static void assertRelsCreated(GraphDatabaseService db) {
        testResult(
                db,
                "MATCH (:Start)-[r:TEST]->(:End) RETURN properties(r) AS props ORDER BY r.myId",
                VectorDbTestUtil::vectorEntityAssertions);
    }

    public static void vectorEntityAssertions(Result r) {
        ResourceIterator<Map> propsIterator = r.columnAs("props");
        assertBerlinProperties(propsIterator.next());
        assertLondonProperties(propsIterator.next());

        assertFalse(propsIterator.hasNext());
    }

    private static void assertLondonProperties(Map props) {
        assertEquals("London", props.get("city"));
        assertEquals("two", props.get("myId"));
        assertTrue(props.get("vect") instanceof float[]);
    }

    private static void assertBerlinProperties(Map props) {
        assertEquals("Berlin", props.get("city"));
        assertEquals("one", props.get("myId"));
        assertTrue(props.get("vect") instanceof float[]);
    }

    public static Map<String, String> getAuthHeader(String key) {
        return map("Authorization", "Bearer " + key);
    }
}
