package apoc.vectordb;

import apoc.util.collection.Iterables;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VectorDbTestUtil {
    
    public static void dropAndDeleteAll(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    public static void assertBerlinResult(Map row, boolean withEntity) {
        assertBerlinResult(row, "1", withEntity);
    }
    
    public static void assertBerlinResult(Map row, String id, boolean withEntity) {
        assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
        assertEquals(id, row.get("id").toString());
        if (withEntity) {
            Map<String, Object> props = ((Entity) row.get("entity")).getAllProperties();
            assertBerlinProperties(props);
        }
    }

    public static void assertLondonResult(Map row, boolean withEntity) {
        assertLondonResult(row, "2", withEntity);
    }

    public static void assertLondonResult(Map row, String id, boolean withEntity) {
        assertEquals(Map.of("city", "London", "foo", "two"), row.get("metadata"));
        assertEquals(id, row.get("id").toString());
        if (withEntity) {
            Map<String, Object> props = ((Entity) row.get("entity")).getAllProperties();
            assertLondonProperties(props);
        }
    }

    public static void assertNodesCreated(GraphDatabaseService db) {
        assertIndexNodesCreated(db);

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                VectorDbTestUtil::vectorEntityAssertions);
    }

    public static void assertIndexNodesCreated(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
            assertEquals(Label.label("Test"), constraints.get(0).getLabel());
            assertEquals(List.of("myId"), constraints.get(0).getPropertyKeys());
        }
    }

    public static void assertRelsAndIndexesCreated(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
            assertEquals(RelationshipType.withName("TEST"), constraints.get(0).getRelationshipType());
            assertEquals(List.of("myId"), constraints.get(0).getPropertyKeys());
        }

        testResult(db, "MATCH (:Start)-[r:TEST]->(:End) RETURN properties(r) AS props ORDER BY r.myId",
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
}
