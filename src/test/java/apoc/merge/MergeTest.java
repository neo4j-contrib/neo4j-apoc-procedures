package apoc.merge;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MergeTest {

    private GraphDatabaseService db;
    public static final Label PERSON = Label.label("Person");

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Merge.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testMergeNode() throws Exception {
        testCall(db, "CALL apoc.merge.node(['Person','Bastard'],{ssid:'123'}, {name:'John'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(true, node.hasLabel(Label.label("Bastard")));
                    assertEquals("John", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                });
    }

    @Test
    public void testMergeNodeWithPreExisting() throws Exception {
        db.execute("CREATE (p:Person{ssid:'123', name:'Jim'})");
        testCall(db, "CALL apoc.merge.node(['Person'],{ssid:'123'}, {name:'John'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals("Jim", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                });

        testResult(db, "match (p:Person) return count(*) as c", result ->
                assertEquals(1, (long)(Iterators.single(result.columnAs("c"))))
        );
    }

    @Test
    public void testMergeRelationships() throws Exception {
        db.execute("create (:Person{name:'Foo'}), (:Person{name:'Bar'})");

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'KNOWS', {rid:123}, {since:'Thu'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'KNOWS', {rid:123}, {since:'Fri'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });
        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'OTHER', null, null, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("OTHER", rel.getType().name());
                    assertTrue(rel.getAllProperties().isEmpty());
                });
    }

}
