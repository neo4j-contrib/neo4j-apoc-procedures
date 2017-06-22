package apoc.nodes;

import apoc.coll.Coll;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 22.06.17
 */
public class GroupingTest {
    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Grouping.class);
        db.execute("CREATE (a:Person {name:'Alice',female:true})-[:KNOWS]->(b:Person {name:'Bob', female:false})<-[:KNOWS]-(c:Person {name:'Cath',female:true})")

                .close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testGroupNode() throws Exception {
        testResult(db, "CALL apoc.nodes.group(['Person'],['female'])",
                (result) -> {
                    assertTrue(result.hasNext());
                    Map<String, Object> row = result.next();
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(1,nodes.size());
                    Node node = nodes.get(0);
                    assertEquals(map("female",true,"count",2L),node.getProperties("count","female"));
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(1,rels.size());
                    Relationship rel = rels.get(0);
                    assertEquals(2L,rel.getProperty("count"));
                    assertEquals("KNOWS",rel.getType().name());
                    assertEquals(map("female",false,"count",1L),rel.getEndNode().getProperties("count","female"));
                    assertTrue(result.hasNext());

                    row = result.next();
                    nodes = (List<Node>) row.get("nodes");
                    assertEquals(1,nodes.size());
                    rels = (List<Relationship>) row.get("relationships");
                    assertEquals(0,rels.size());
                    node = nodes.get(0);
                    assertEquals(map("female",false,"count",1L),node.getProperties("count","female"));
                });
    }
}
