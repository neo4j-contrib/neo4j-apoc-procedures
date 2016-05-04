package apoc.refactor;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 25.03.16
 */
public class GraphRefactoringTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, GraphRefactoring.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExtractNode() throws Exception {
        Long id = db.execute("CREATE (f:Foo)-[rel:FOOBAR {a:1}]->(b:Bar) RETURN id(rel) as id").<Long>columnAs("id").next();
        testCall(db, "CALL apoc.refactor.extractNode({ids},['FooBar'],'FOO','BAR')", map("ids", asList(id)),
                (r) -> {
                    assertEquals(id, r.get("input"));
                    Node node = (Node) r.get("output");
                    assertEquals(true, node.hasLabel(Label.label("FooBar")));
                    assertEquals(1L, node.getProperty("a"));
                    assertNotNull(node.getSingleRelationship(RelationshipType.withName("FOO"), Direction.OUTGOING));
                    assertNotNull(node.getSingleRelationship(RelationshipType.withName("BAR"), Direction.INCOMING));
                });

    }
    @Test
    public void testCollapseNode() throws Exception {
        Long id = db.execute("CREATE (f:Foo)-[:FOO {a:1}]->(b:Bar {c:3})-[:BAR {b:2}]->(f) RETURN id(b) as id").<Long>columnAs("id").next();
        testCall(db, "CALL apoc.refactor.collapseNode({ids},'FOOBAR')", map("ids", asList(id)),
                (r) -> {
                    assertEquals(id, r.get("input"));
                    Relationship rel = (Relationship) r.get("output");
                    assertEquals(true, rel.isType(RelationshipType.withName("FOOBAR")));
                    assertEquals(1L, rel.getProperty("a"));
                    assertEquals(2L, rel.getProperty("b"));
                    assertEquals(3L, rel.getProperty("c"));
                    assertNotNull(rel.getEndNode().hasLabel(Label.label("Foo")));
                    assertNotNull(rel.getStartNode().hasLabel(Label.label("Foo")));
                });

    }

    @Test
    public void testCloneNodes() throws Exception {
//        TestUtil.testCall(db,
//                "",
//                (row) -> {
//
//        });
    }

    @Test
    public void testMergeNodes() throws Exception {

    }

    @Test
    public void testChangeType() throws Exception {

    }

    @Test
    public void testRedirectRelationship() throws Exception {

    }
}
