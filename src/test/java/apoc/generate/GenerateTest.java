package apoc.generate;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;

/**
 * Integration test for {@link Generate}.
 */
public class GenerateTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Generate.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldGenerateErdosRenyi1() {
        db.execute("CALL apoc.generate.er(20,100,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(20, count(db.getAllNodes()));
            assertEquals(100, count(db.getAllRelationships()));
            assertEquals("Person", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("FRIEND_OF", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("name"));

            tx.success();
        }
    }

    @Test
    public void shouldGenerateErdosRenyi2() {
        db.execute("CALL apoc.generate.er(null,null,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(1000, count(db.getAllNodes()));
            assertEquals(10000, count(db.getAllRelationships()));
            assertEquals("Person", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("FRIEND_OF", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("name"));

            tx.success();
        }
    }

    @Test
    public void shouldGenerateErdosRenyi3() {
        db.execute("CALL apoc.generate.er(20,100,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(20, count(db.getAllNodes()));
            assertEquals(100, count(db.getAllRelationships()));
            assertEquals("TestLabel", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("TEST_REL", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("uuid"));

            tx.success();
        }
    }

    @Test
    public void shouldGenerateBarabasiAlbert1() {
        db.execute("CALL apoc.generate.ba(100,3,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(100, count(db.getAllNodes()));
            assertEquals(294, count(db.getAllRelationships()));
            assertEquals("Person", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("FRIEND_OF", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("name"));
            tx.success();
        }
    }

    @Test
    public void shouldGenerateBarabasiAlbert2() {
        db.execute("CALL apoc.generate.ba(100,3,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(100, count(db.getAllNodes()));
            assertEquals(294, count(db.getAllRelationships()));
            assertEquals("TestLabel", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("TEST_REL", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("uuid"));
            tx.success();
        }
    }

    @Test
    public void shouldGenerateBarabasiAlbert3() {
        db.execute("CALL apoc.generate.ba(null,null,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(1000, count(db.getAllNodes()));
            assertEquals(1997, count(db.getAllRelationships()));
            assertEquals("TestLabel", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("TEST_REL", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("uuid"));
            tx.success();
        }
    }

    @Test
    public void shouldGenerateWattsStrogatz1() {
        db.execute("CALL apoc.generate.ws(100,10,0.5,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(100, count(db.getAllNodes()));
            assertEquals(500, count(db.getAllRelationships()));
            assertEquals("Person", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("FRIEND_OF", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("name"));
            tx.success();
        }
    }

    @Test
    public void shouldGenerateWattsStrogatz2() {
        db.execute("CALL apoc.generate.ws(null,null,null,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(1000, count(db.getAllNodes()));
            assertEquals(2000, count(db.getAllRelationships()));
            assertEquals("Person", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("FRIEND_OF", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("name"));
            tx.success();
        }
    }

    @Test
    public void shouldGenerateWattsStrogatz3() {
        db.execute("CALL apoc.generate.ws(null,null,null,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(1000, count(db.getAllNodes()));
            assertEquals(2000, count(db.getAllRelationships()));
            assertEquals("TestLabel", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("TEST_REL", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("uuid"));
            tx.success();
        }
    }

    @Test
    public void shouldGenerateComplete1() {
        db.execute("CALL apoc.generate.complete(10,'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(10, count(db.getAllNodes()));
            assertEquals(45, count(db.getAllRelationships()));
            assertEquals("TestLabel", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("TEST_REL", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("uuid"));
            tx.success();
        }
    }

    @Test
    public void shouldGenerateComplete2() {
        db.execute("CALL apoc.generate.complete(10,null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(10, count(db.getAllNodes()));
            assertEquals(45, count(db.getAllRelationships()));
            assertEquals("Person", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("FRIEND_OF", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("name"));
            tx.success();
        }
    }

    @Test
    public void shouldGenerateSimple1() {
        db.execute("CALL apoc.generate.simple([2,2,2,2],'TestLabel','TEST_REL')");

        try (Transaction tx = db.beginTx()) {
            assertEquals(4, count(db.getAllNodes()));
            assertEquals(4, count(db.getAllRelationships()));
            assertEquals("TestLabel", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("TEST_REL", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("uuid"));
            tx.success();
        }
    }

    @Test
    public void shouldGenerateSimple2() {
        db.execute("CALL apoc.generate.simple([2,2,2,2],null,null)");

        try (Transaction tx = db.beginTx()) {
            assertEquals(4, count(db.getAllNodes()));
            assertEquals(4, count(db.getAllRelationships()));
            assertEquals("Person", firstOrNull(firstOrNull(db.getAllNodes()).getLabels()).name());
            assertEquals("FRIEND_OF", firstOrNull(db.getAllRelationships()).getType().name());
            assertTrue(firstOrNull(db.getAllNodes()).hasProperty("name"));
            tx.success();
        }
    }
}
