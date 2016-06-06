package apoc.meta;

import apoc.meta.Meta;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.traversal.Evaluators.toDepth;

public class MetaTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Meta.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    /*
        @Test public void testMetaStats() throws Exception {
            testResult(db,"CALL apoc.meta.stats", (r) -> assertEquals(true, r.hasNext()));
        }
    */
    @Test
    public void testMetaType() throws Exception {
        try (Transaction tx = db.beginTx()) {
            Node node = db.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("FOO"));
            testTypeName(node, "NODE");
            testTypeName(rel, "RELATIONSHIP");
            Path path = db.traversalDescription().evaluator(toDepth(1)).traverse(node).iterator().next();
            testTypeName(path, "PATH");
            tx.failure();
        }
        testTypeName(singletonMap("a", 10), "MAP");
        testTypeName(asList(1, 2), "LIST");
        testTypeName(1L, "INTEGER");
        testTypeName(1, "INTEGER");
        testTypeName(1.0D, "FLOAT");
        testTypeName(1.0, "FLOAT");
        testTypeName("a", "STRING");
        testTypeName(false, "BOOLEAN");
        testTypeName(true, "BOOLEAN");
        testTypeName(null, "NULL");
    }

    @Test
    public void testMetaIsType() throws Exception {
        try (Transaction tx = db.beginTx()) {
            Node node = db.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("FOO"));
            testIsTypeName(node, "NODE");
            testIsTypeName(rel, "RELATIONSHIP");
            Path path = db.traversalDescription().evaluator(toDepth(1)).traverse(node).iterator().next();
            testIsTypeName(path, "PATH");
            tx.failure();
        }
        testIsTypeName(singletonMap("a", 10), "MAP");
        testIsTypeName(asList(1, 2), "LIST");
        testIsTypeName(1L, "INTEGER");
        testIsTypeName(1, "INTEGER");
        testIsTypeName(1.0D, "FLOAT");
        testIsTypeName(1.0, "FLOAT");
        testIsTypeName("a", "STRING");
        testIsTypeName(false, "BOOLEAN");
        testIsTypeName(true, "BOOLEAN");
        testIsTypeName(null, "NULL");
    }

    private void testTypeName(Object value, String type) {
        TestUtil.testCall(db, "CALL apoc.meta.type", singletonMap("value", value), row -> assertEquals(type, row.get("value")));
    }

    private void testIsTypeName(Object value, String type) {
        TestUtil.testResult(db, "CALL apoc.meta.isType", map("value", value, "type", type), result -> assertEquals(true, result.hasNext()));
        TestUtil.testResult(db, "CALL apoc.meta.isType", map("value", value, "type", type + "foo"), result -> assertEquals(false, result.hasNext()));
    }

    @Test
    public void testMetaStats() throws Exception {
        db.execute("CREATE (:Actor)-[:ACTED_IN]->(:Movie) ").close();
        TestUtil.testCall(db, "CALL apoc.meta.stats()", r -> {
            assertEquals(2L,r.get("labelCount"));
            assertEquals(1L,r.get("relTypeCount"));
            assertEquals(0L,r.get("propertyKeyCount"));
            assertEquals(2L,r.get("nodeCount"));
            assertEquals(1L,r.get("relCount"));
            assertEquals(map("Actor",1L,"Movie",1L),r.get("labels"));
            assertEquals(map(
                    "(:Actor)-[:ACTED_IN]->()",1L,
                    "()-[:ACTED_IN]->(:Movie)",1L,
                    "()-[:ACTED_IN]->()",1L),r.get("relTypes"));
        });
    }
    @Test
    public void testMetaGraph() throws Exception {
        db.execute("CREATE (:Actor)-[:ACTED_IN]->(:Movie) ").close();
        TestUtil.testCall(db, "CALL apoc.meta.graph()",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    Node n1 = nodes.get(0);
                    assertEquals(true, n1.hasLabel(Label.label("Actor")));
                    assertEquals(1, n1.getProperty("count"));
                    assertEquals("Actor", n1.getProperty("name"));
                    Node n2 = nodes.get(1);
                    assertEquals(true, n2.hasLabel(Label.label("Movie")));
                    assertEquals("Movie", n2.getProperty("name"));
                    assertEquals(1, n1.getProperty("count"));
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    Relationship rel = rels.iterator().next();
                    assertEquals("ACTED_IN", rel.getType().name());
                    assertEquals(1, rel.getProperty("count"));
                });
    }

    @Test
    public void testMetaGraph2() throws Exception {
        db.execute("CREATE (:Actor)-[:ACTED_IN]->(:Movie) ").close();
        TestUtil.testCall(db, "CALL apoc.meta.graphSample(100)",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    Node n1 = nodes.get(0);
                    assertEquals(true, n1.hasLabel(Label.label("Actor")));
                    assertEquals(1, n1.getProperty("count"));
                    assertEquals("Actor", n1.getProperty("name"));
                    Node n2 = nodes.get(1);
                    assertEquals(true, n2.hasLabel(Label.label("Movie")));
                    assertEquals("Movie", n2.getProperty("name"));
                    assertEquals(1, n1.getProperty("count"));
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    Relationship rel = rels.iterator().next();
                    assertEquals("ACTED_IN", rel.getType().name());
                    assertEquals(1, rel.getProperty("count"));
                });
    }

    @Test
    public void testMetaData() throws Exception {
        db.execute("create index on :Movie(title)").close();
        db.execute("create constraint on (a:Actor) assert a.name is unique").close();
        db.execute("CREATE (:Actor {name:'Tom Hanks'})-[:ACTED_IN {roles:'Forrest'}]->(:Movie {title:'Forrest Gump'}) ").close();
        TestUtil.testResult(db, "CALL apoc.meta.data()",
                (r) -> {
                    int count = 0;
                    while (r.hasNext()) {
                        Map<String, Object> row = r.next();
                        // todo more assertions
                        System.out.println(row);
                        count ++;
                    }
                    assertEquals(5,count);
                });
    }
}
