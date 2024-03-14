package apoc.vectordb;

import apoc.util.collection.Iterables;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCallCount;
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
    
    public static void assertBerlinVector(Map row) {
        assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
        assertEquals("1", row.get("id").toString());
    }

    public static void assertLondonVector(Map row) {
        assertEquals(Map.of("city", "London", "foo", "two"), row.get("metadata"));
        assertEquals("2", row.get("id").toString());
    }
    
    public static void assertOtherNodesCreated(GraphDatabaseService db) {
        assertIndexNodesCreated(db);

        testCallCount(db, "MATCH (n:Test) RETURN n", 4);
    }

    public static void assertNodesCreated(GraphDatabaseService db, boolean isNew) {
        assertIndexNodesCreated(db);

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                r -> vectorEntityAssertions(r, isNew));
    }

    public static void assertIndexNodesCreated(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.stream(tx.schema().getIndexes())
                    .filter(i -> i.getIndexType().equals(IndexType.VECTOR))
                    .toList();
            assertEquals(1, indexes.size());
            assertEquals(List.of(Label.label("Test")), indexes.get(0).getLabels());
            assertEquals(List.of("vect"), indexes.get(0).getPropertyKeys());

            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
            assertEquals(Label.label("Test"), constraints.get(0).getLabel());
            assertEquals(List.of("myId"), constraints.get(0).getPropertyKeys());
        }
    }

    public static void assertRelsAndIndexesCreated(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.stream(tx.schema().getIndexes())
                    .filter(i -> i.getIndexType().equals(IndexType.VECTOR))
                    .toList();
            assertEquals(1, indexes.size());
            assertEquals(List.of(RelationshipType.withName("TEST")), indexes.get(0).getRelationshipTypes());
            assertEquals(List.of("vect"), indexes.get(0).getPropertyKeys());

            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
            assertEquals(RelationshipType.withName("TEST"), constraints.get(0).getRelationshipType());
            assertEquals(List.of("myId"), constraints.get(0).getPropertyKeys());
        }

        testResult(db, "MATCH (:Start)-[r:TEST]->(:End) RETURN properties(r) AS props ORDER BY r.myId",
                r -> vectorEntityAssertions(r, false));
    }

    public static void vectorEntityAssertions(Result r, boolean isNew) {
        ResourceIterator<Map> props = r.columnAs("props");
        Map next = props.next();
        assertEquals("Berlin", next.get("city"));
        if (!isNew) {
            assertEquals("one", next.get("myId"));
        }
        assertTrue(next.get("vect") instanceof float[]);
        next = props.next();
        assertEquals("London", next.get("city"));
        if (!isNew) {
            assertEquals("two", next.get("myId"));
        }
        assertTrue(next.get("vect") instanceof float[]);

        assertFalse(props.hasNext());
    }
}
