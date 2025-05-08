package apoc.create;

import apoc.util.ExtendedTestUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.result.VirtualNode.ERROR_NODE_NULL;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.Label.label;

public class CreateExtendedTest {
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() {
        TestUtil.registerProcedure(db, Create.class, CreateExtended.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }
    
    @Test
    public void testVirtualFromNodeFunction() {
        testCall(
                db,
                """
                        CREATE (n:Person{name:'Vincent', born: 1974} )
                        RETURN apoc.create.virtual.fromNodeExtended(n, ['name']) AS node
                        """,
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertTrue(node.getId() < 0);
                    commonCreateNodeAssertions(node);
                });
    }

    @Test
    public void testVirtualFromNodeWithAdditionalPropertiesFunction() {
        testCall(
                db,
                """
                        CREATE (n:Person {name:'Vincent', born: 1974} )
                        RETURN apoc.create.virtual.fromNodeExtended(n, ['name'], {alpha: 1, foo: 'bar'}) AS node
                        """,
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(1L, node.getProperty("alpha"));
                    assertEquals("bar", node.getProperty("foo"));
                    assertTrue(node.getId() < 0);
                    commonCreateNodeAssertions(node);
                });
    }

    @Test
    public void testVirtualFromNodeShouldNotEditOriginalOne() {
        db.executeTransactionally("CREATE (n:Person {name:'toUpdate'})");

        testCall(
                db,
                """
                        MATCH (n:Person {name:'toUpdate'})
                        WITH apoc.create.virtual.fromNodeExtended(n, ['name']) as nVirtual
                        CALL apoc.create.setProperty(nVirtual, 'ajeje', 0) YIELD node RETURN node
                        """,
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals("toUpdate", node.getProperty("name"));
                    assertEquals(0L, node.getProperty("ajeje"));
                });

        testCall(
                db,
                """
                        MATCH (n:Person {name:'toUpdate'})
                        WITH apoc.create.virtual.fromNodeExtended(n, ['name']) as node
                        SET node.ajeje = 0 RETURN node""",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals("toUpdate", node.getProperty("name"));
                    assertFalse(node.hasProperty("ajeje"));
                });

        testCall(db, "MATCH (node:Person {name:'toUpdate'}) RETURN node", (row) -> {
            Node node = (Node) row.get("node");
            assertEquals("toUpdate", node.getProperty("name"));
            assertFalse(node.hasProperty("ajeje"));
        });
    }

    @Test
    public void testValidationNodes() {
        ExtendedTestUtil.assertFails(db, 
                "RETURN apoc.create.virtual.fromNodeExtended(null, ['name']) as node", 
                Map.of(),
                ERROR_NODE_NULL);
    }

    private static void commonCreateNodeAssertions(Node node) {
        assertTrue(node.hasLabel(label("Person")));
        assertEquals("Vincent", node.getProperty("name"));
        assertNull(node.getProperty("born"));
    }
}
