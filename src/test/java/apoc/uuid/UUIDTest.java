package apoc.uuid;

import apoc.util.TestUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author ab-larus
 * @since 05.09.18
 */
public class UUIDTest {

    private static GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder()
                .setConfig("apoc.uuid.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Uuid.class);
        db.execute("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE").close();
        db.execute("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.add('Bar') YIELD label RETURN label");
        db.execute("CREATE (n:Bar)").close();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testUUID() throws Exception {
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Bar) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
            tx.success();
        }
        testConstraint();
    }

    @Test
    public void testUUIDWhithoutRemovedUuid() throws Exception {
        db.execute("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})").close(); // Create the uuid manually and except is the same after the trigger
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid")); // Check if the uuid if the same when created
            tx.success();
        }
        testConstraint();
    }

    @Test
    public void testUUIDSetUuidToEmptyAndRestore() throws Exception {
        db.execute("CALL apoc.uuid.add('Test') YIELD label RETURN label").close();
        db.execute("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})").close();
        db.execute("MATCH (t:Test) SET t.uuid = ''").close();
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
            tx.success();
        }
        testConstraint();
    }

    @Test
    public void testUUIDDeleteUuidAndRestore() throws Exception {
        db.execute("CALL apoc.uuid.add('Test') YIELD label RETURN label").close();
        db.execute("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})").close();
        db.execute("MATCH (t:Test) remove t.uuid").close();
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
            tx.success();
        }
        testConstraint();
    }

    @Test
    public void testUUIDSetUuidToEmpty() throws Exception {
        db.execute("CALL apoc.uuid.add('Test') YIELD label RETURN label").close();
        db.execute("CREATE (n:Test:Empty {name:'empty'})").close();
        db.execute("MATCH (t:Test:Empty) SET t.uuid = ''").close();
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Empty) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
            tx.success();
        }
        testConstraint();
    }

    @Test
    public void testUUIDList() throws Exception {
        TestUtil.testCall(db, "CALL apoc.uuid.list()", (row) -> assertResult(row, "Bar", true));
        testConstraint();
    }

    @Test
    public void testAddRemoveUuid() throws Exception {
        db.execute("CALL apoc.uuid.add('Test') YIELD label RETURN label");
        TestUtil.testCall(db, "CALL apoc.uuid.remove('Bar')", (row) -> assertResult(row, "Bar", false));
        TestUtil.testCall(db, "CALL apoc.uuid.list()", (row) -> assertResult(row, "Test", true));
        TestUtil.testCall(db, "CALL apoc.uuid.remove('Test')", (row) -> assertResult(row, "Test", false));
    }

    @Test
    public void testAddRemoveUuidAfterPhase() throws Exception {
        db.execute("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.add('Person') YIELD label RETURN label").close();
        db.execute("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})").close();
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (person:Person) return person").next().get("person");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
            tx.success();
        }
    }

    @Test
    public void testNotaddToAlreadyPresent() throws Exception {
        db.execute("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})").close();
        db.execute("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.add('Person', {addToAlreadyPresent: false}) YIELD label RETURN label").close();
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (person:Person) return person").next().get("person");
            assertFalse(n.getAllProperties().containsKey("uuid"));
            tx.success();
        }
    }

    @Test
    public void testaddToAlreadyPresent() throws Exception {
        db.execute("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})").close();
        db.execute("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.add('Person') YIELD label RETURN label").close();
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (person:Person) return person").next().get("person");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
            tx.success();
        }
    }

    @Test
    public void testRemoveAllUuid() {
        TestUtil.testCall(db, "CALL apoc.uuid.removeAll()", (row) -> assertResult(row, "Bar", false));
    }

    @Test(expected = RuntimeException.class)
    public void testAddWithError() {
        try {
            db.execute("CALL apoc.uuid.add('Wrong') YIELD label RETURN label");
        } catch (RuntimeException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("No constraint found for label: Wrong, please add the constraint with the following : `CREATE CONSTRAINT ON (wrong:Wrong) ASSERT wrong.uuid IS UNIQUE`", except.getMessage());
            throw e;
        }
    }

    private void testConstraint() {
        try (ResourceIterator<String> result = db.execute("CALL db.constraints").columnAs("description")) {
            assertTrue(result.hasNext());
            assertEquals("CONSTRAINT ON ( bar:Bar ) ASSERT bar.uuid IS UNIQUE", result.next());
            assertEquals("CONSTRAINT ON ( test:Test ) ASSERT test.uuid IS UNIQUE", result.next());
            assertTrue(!result.hasNext());
        }
    }

    private void assertResult(Map<String, Object> row, String labels, boolean installed) {
        assertEquals(labels, row.get("label"));
        assertEquals(installed, row.get("installed"));
    }

}