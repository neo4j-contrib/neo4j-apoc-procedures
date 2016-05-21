package apoc.create;

import apoc.create.Create;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class CreateTest {

    private GraphDatabaseService db;
    public static final Label PERSON = Label.label("Person");

    @Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Create.class);
    }
    @After public void tearDown() {
        db.shutdown();
    }

    @Test public void testCreateNode() throws Exception {
        testCall(db, "CALL apoc.create.node(['Person'],{name:'John'})",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals("John", node.getProperty("name"));
                });
    }
    @Test public void testVirtualNode() throws Exception {
        testCall(db, "CALL apoc.create.vNode(['Person'],{name:'John'})",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals("John", node.getProperty("name"));
                });
    }
    @Test public void testCreateNodes() throws Exception {
        testResult(db, "CALL apoc.create.nodes(['Person'],[{name:'John'},{name:'Jane'}])",
                 (res) -> {
                     Node node = (Node) res.next().get("node");
                    assertEquals(true, node.hasLabel(PERSON));
                    assertEquals("John", node.getProperty("name"));

                    node = (Node) res.next().get("node");
                    assertEquals(true, node.hasLabel(PERSON));
                    assertEquals("Jane", node.getProperty("name"));
                });
    }
    @Test public void testCreateRelationship() throws Exception {
        testCall(db, "CREATE (n),(m) WITH n,m CALL apoc.create.relationship(n,'KNOWS',{since:2010}, m) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals(true, rel.isType(RelationshipType.withName("KNOWS")));
                    assertEquals(2010L, rel.getProperty("since"));
                });
    }

    @Test public void testCreateVirtualRelationship() throws Exception {
        testCall(db, "CREATE (n),(m) WITH n,m CALL apoc.create.vRelationship(n,'KNOWS',{since:2010}, m) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals(true, rel.isType(RelationshipType.withName("KNOWS")));
                    assertEquals(2010L, rel.getProperty("since"));
                });
    }
    @Test public void testCreatePattern() throws Exception {
        testCall(db, "CALL apoc.create.vPattern({_labels:['Person'],name:'John'},'KNOWS',{since:2010},{_labels:['Person'],name:'Jane'})",
                (row) -> {
                    Node john = (Node) row.get("from");
                    assertEquals(true, john.hasLabel(PERSON));
                    assertEquals("John", john.getProperty("name"));
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals(true, rel.isType(RelationshipType.withName("KNOWS")));
                    assertEquals(2010L, rel.getProperty("since"));
                    Node jane = (Node) row.get("to");
                    assertEquals(true, jane.hasLabel(PERSON));
                    assertEquals("Jane", jane.getProperty("name"));
                });
    }
    @Test public void testCreatePatternFull() throws Exception {
        testCall(db, "CALL apoc.create.vPatternFull(['Person'],{name:'John'},'KNOWS',{since:2010},['Person'],{name:'Jane'})",
                (row) -> {
                    Node john = (Node) row.get("from");
                    assertEquals(true, john.hasLabel(PERSON));
                    assertEquals("John", john.getProperty("name"));
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals(true, rel.isType(RelationshipType.withName("KNOWS")));
                    assertEquals(2010L, rel.getProperty("since"));
                    Node jane = (Node) row.get("to");
                    assertEquals(true, jane.hasLabel(PERSON));
                    assertEquals("Jane", jane.getProperty("name"));
                });
    }
}
