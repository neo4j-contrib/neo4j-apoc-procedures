package apoc.uuid;

import apoc.ApocSettings;
import apoc.create.Create;
import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author ab-larus
 * @since 05.09.18
 */
public class UUIDTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.auth_enabled, true)
            .withSetting(ApocSettings.apoc_uuid_enabled, true);

    private static final String UUID_TEST_REGEXP = "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Uuid.class, Create.class, Periodic.class);
    }

    @Test
    public void testUUID() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (p:Person) ASSERT p.uuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Person') YIELD label RETURN label");

        // when
        db.executeTransactionally("CREATE (p:Person{name:'Daniel'})-[:WORK]->(c:Company{name:'Neo4j'})");

        // then
        try (Transaction tx = db.beginTx()) {
            Node company = (Node) tx.execute("MATCH (c:Company) return c").next().get("c");
            assertTrue(!company.hasProperty("uuid"));
            Node person = (Node) tx.execute("MATCH (p:Person) return p").next().get("p");
            assertTrue(person.getAllProperties().containsKey("uuid"));

            assertTrue(person.getAllProperties().get("uuid").toString().matches(UUID_TEST_REGEXP));
            tx.commit();
        }
    }
    
    @Test
    public void testUUIDWithSetLabel() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (p:Mario) ASSERT p.uuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Mario', {addToSetLabels: true}) YIELD label RETURN label");
        // when
        db.executeTransactionally("CREATE (p:Luigi {foo:'bar'}) SET p:Mario");
        // then
        TestUtil.testCall(db, "MATCH (a:Luigi:Mario) RETURN a.uuid as uuid",
                row -> assertTrue(((String) row.get("uuid")).matches(UUID_TEST_REGEXP)));

        // - set after creation
        db.executeTransactionally("CREATE (:Peach)");
        // when
        db.executeTransactionally("MATCH (p:Peach) SET p:Mario");
        // then
        TestUtil.testCall(db, "MATCH (a:Peach:Mario) RETURN a.uuid as uuid", 
                row -> assertTrue(((String) row.get("uuid")).matches(UUID_TEST_REGEXP)));

        TestUtil.testCall(db, "CALL apoc.uuid.remove('Mario')",
                (row) -> assertResult(row, "Mario", false,
                        Util.map("uuidProperty", "uuid", "addToSetLabels", true)));
    }

    @Test
    public void testUUIDWithoutRemovedUuid() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Test') YIELD label RETURN label");

        // when
        db.executeTransactionally("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})"); // Create the uuid manually and except is the same after the trigger

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid")); // Check if the uuid if the same when created
            tx.commit();
        }
    }

    @Test
    public void testUUIDSetUuidToEmptyAndRestore() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Test') YIELD label RETURN label");
        db.executeTransactionally("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})");

        // when
        db.executeTransactionally("MATCH (t:Test) SET t.uuid = ''");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void testUUIDDeleteUuidAndRestore() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Test') YIELD label RETURN label");
        db.executeTransactionally("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})");

        // when
        db.executeTransactionally("MATCH (t:Test) remove t.uuid");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void testUUIDSetUuidToEmpty() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Test') YIELD label RETURN label");
        db.executeTransactionally("CREATE (n:Test:Empty {name:'empty'})");

        // when
        db.executeTransactionally("MATCH (t:Test:Empty) SET t.uuid = ''");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:Empty) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
            tx.commit();
        }
    }

    @Test
    public void testUUIDList() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE");

        // when
        db.executeTransactionally("CALL apoc.uuid.install('Bar') YIELD label RETURN label");

        // then
        TestUtil.testCall(db, "CALL apoc.uuid.list()",
                (row) -> assertResult(row, "Bar", true,
                        Util.map("uuidProperty", "uuid", "addToSetLabels", false)));
    }

    @Test
    public void testUUIDListAddToExistingNodes() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE");
        db.executeTransactionally("UNWIND Range(1,10) as i CREATE(bar:Bar{id: i})");

        // when
        db.executeTransactionally("CALL apoc.uuid.install('Bar')");

        // then
        List<String> uuidList = TestUtil.firstColumn(db, "MATCH (n:Bar) RETURN n.uuid AS uuid");
        assertEquals(10, uuidList.size());
        assertTrue(uuidList.stream().allMatch(uuid -> uuid.matches(UUID_TEST_REGEXP)));
    }

    @Test
    public void testAddRemoveUuid() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.foo IS UNIQUE");

        // when
        db.executeTransactionally("CALL apoc.uuid.install('Test', {uuidProperty: 'foo'}) YIELD label RETURN label");

        // then
        TestUtil.testCall(db, "CALL apoc.uuid.list()",
                (row) -> assertResult(row, "Test", true,
                        Util.map("uuidProperty", "foo", "addToSetLabels", false)));
        TestUtil.testCall(db, "CALL apoc.uuid.remove('Test')",
                (row) -> assertResult(row, "Test", false,
                        Util.map("uuidProperty", "foo", "addToSetLabels", false)));
    }

    @Test
    public void testNotAddToExistingNodes() {
        // given
        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");

        // when
        db.executeTransactionally("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Person', {addToExistingNodes: false}) YIELD label RETURN label");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (person:Person) return person").next().get("person");
            assertFalse(n.getAllProperties().containsKey("uuid"));
            tx.commit();
        }
    }

    @Test
    public void testAddToExistingNodes() {
        // given
        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");

        // when
        db.executeTransactionally("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Person') YIELD label RETURN label");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (person:Person) return person").next().get("person");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
            tx.commit();
        }
    }

    @Test
    public void testAddToExistingNodesBatchResult() {
        // given
        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");

        // when
        db.executeTransactionally("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE");

        // then
        try (Transaction tx = db.beginTx()) {
            long total = (Long) tx.execute(
                    "CALL apoc.uuid.install('Person') YIELD label, installed, properties, batchComputationResult " +
                            "RETURN batchComputationResult.total as total")
                    .next()
                    .get("total");
            assertEquals(1, total);
            tx.commit();
        }
    }

    @Test
    public void testRemoveAllUuid() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.foo IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Bar') YIELD label RETURN label");
        db.executeTransactionally("CALL apoc.uuid.install('Test', {addToExistingNodes: false, uuidProperty: 'foo'}) YIELD label RETURN label");

        // when
        TestUtil.testResult(db, "CALL apoc.uuid.removeAll()",
                (result) -> {
                    // then
                    Map<String, Object> row = result.next();
                    assertResult(row, "Test", false,
                            Util.map("uuidProperty", "foo", "addToSetLabels", false));
                    row = result.next();
                    assertResult(row, "Bar", false,
                            Util.map("uuidProperty", "uuid", "addToSetLabels", false));
                });
    }

    @Test(expected = RuntimeException.class)
    public void testAddWithError() {
        try {
            // when
            db.executeTransactionally("CALL apoc.uuid.install('Wrong') YIELD label RETURN label");
        } catch (RuntimeException e) {
            // then
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("No constraint found for label: Wrong, please add the constraint with the following : `CREATE CONSTRAINT ON (wrong:Wrong) ASSERT wrong.uuid IS UNIQUE`", except.getMessage());
            throw e;
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAddWithErrorAndCustomField() {
        try {
            // when
            db.executeTransactionally("CALL apoc.uuid.install('Wrong', {uuidProperty: 'foo'}) YIELD label RETURN label");
        } catch (RuntimeException e) {
            // then
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("No constraint found for label: Wrong, please add the constraint with the following : `CREATE CONSTRAINT ON (wrong:Wrong) ASSERT wrong.foo IS UNIQUE`", except.getMessage());
            throw e;
        }
    }

    private void assertResult(Map<String, Object> row, String labels, boolean installed, Map<String, Object> conf) {
        assertEquals(labels, row.get("label"));
        assertEquals(installed, row.get("installed"));
        assertEquals(conf, row.get("properties"));
    }

}
