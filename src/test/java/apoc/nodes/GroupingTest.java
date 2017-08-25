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
        db.execute("CREATE (a:Person {name:'Alice',female:true,age:32,kids:1})-[:KNOWS]->(b:Person {name:'Bob', female:false, age:42,kids:3})<-[:KNOWS]-(c:Person {name:'Cath',female:true,age:28,kids:2})")

                .close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testGroupNode() throws Exception {
        Map<String, Object> female = map("female", true, "count_*", 2L, "sum_kids", 3L, "min_age", 28L, "max_age", 32L, "avg_age", 30D);
        Map<String, Object> male = map("female", false, "count_*", 1L, "sum_kids", 3L, "min_age", 42L, "max_age", 42L, "avg_age", 42D);
        testResult(db, "CALL apoc.nodes.group(['Person'],['female'],[{`*`:'count',kids:'sum',age:['min','max','avg'],gender:'collect'},{`*`:'count'}])",
                (result) -> {
                    assertTrue(result.hasNext());
                    Map<String, Object> row = result.next();
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(1,nodes.size());
                    Node node = nodes.get(0);
                    String[] keys = {"count_*", "female", "sum_kids", "min_age", "max_age", "avg_age"};
                    assertEquals(node.getProperty("female").equals(true) ? female : male, node.getProperties(keys));
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(1,rels.size());
                    Relationship rel = rels.get(0);
                    assertEquals(2L,rel.getProperty("count_*"));
                    assertEquals("KNOWS",rel.getType().name());
                    node = rel.getOtherNode(node);
                    assertEquals(node.getProperty("female").equals(true) ? female : male, node.getProperties(keys));
                    assertTrue(result.hasNext());

                    row = result.next();
                    nodes = (List<Node>) row.get("nodes");
                    assertEquals(1,nodes.size());
                    node = nodes.get(0);
                    assertEquals(node.getProperty("female").equals(true) ? female : male, node.getProperties(keys));

                    rels = (List<Relationship>) row.get("relationships");
                    assertEquals(0,rels.size());
                });
    }
}
