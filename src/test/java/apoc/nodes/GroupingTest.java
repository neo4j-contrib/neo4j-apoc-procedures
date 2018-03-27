package apoc.nodes;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        db.execute("CREATE " +
          "(alice:Person {name:'Alice', gender:'female', age:32, kids:1})," +
          "(bob:Person   {name:'Bob',   gender:'male',   age:42, kids:3})," +
          "(eve:Person   {name:'Eve',   gender:'female', age:28, kids:2})," +
          "(graphs:Forum {name:'Graphs',    members:23})," +
          "(dbs:Forum    {name:'Databases', members:42})," +
          "(alice)-[:KNOWS {since:2017}]->(bob)," +
          "(eve)-[:KNOWS   {since:2018}]->(bob)," +
          "(alice)-[:MEMBER_OF]->(graphs)," +
          "(alice)-[:MEMBER_OF]->(dbs)," +
          "(bob)-[:MEMBER_OF]->(dbs)," +
          "(eve)-[:MEMBER_OF]->(graphs)");
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testGroupAllNodes() throws Exception {
      Map<String, Object> female = map("gender", "female", "count_*", 2L, "min_age", 28L);
      Map<String, Object> male = map("gender", "male", "count_*", 1L, "min_age", 42L);
      Map<String, Object> other = map("gender", null, "count_*", 2L);

      testResult(db, "CALL apoc.nodes.group(" +
          "['*'],['gender'],[" +
            "{`*`:'count', age:'min'}," +
            "{`*`:'count'}" +
          "])",
        (result) -> {
          assertTrue(result.hasNext());

          String[] keys = {"count_*", "gender", "min_age"};
          while (result.hasNext()) {
            Map<String, Object> row = result.next();
            List<Node> nodes = (List<Node>) row.get("nodes");

            assertEquals(1, nodes.size());
            Node node = nodes.get(0);
            Object value = node.getProperty("gender");

            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            if (value == null) {
              assertEquals(other, node.getProperties(keys));
              assertEquals(0L, rels.size());
            } else if (value.equals("female")) {
              assertEquals(female, node.getProperties(keys));
              assertEquals(2L, rels.size());
              Relationship rel = rels.get(0);
              Object count = rel.getProperty("count_*");
              if (count.equals(3L)) { // MEMBER_OF
                assertEquals(other, rel.getEndNode().getProperties(keys));
              } else if (count.equals(2L)) { // KNOWS
                assertEquals(male, rel.getEndNode().getProperties(keys));
              } else {
                assertTrue("Unexpected count value: " + count, false);
              }
            } else if (value.equals("male")) {
              assertEquals(male, node.getProperties(keys));
              assertEquals(1L, rels.size());
              Relationship rel = rels.get(0); // MEMBER_OF
              assertEquals(1L, rel.getProperty("count_*"));
              assertEquals(other, rel.getEndNode().getProperties(keys));
            } else {
              assertTrue("Unexpected value: " + value, false);
            }
          }
        });
    }

    @Test
    public void testGroupNode() throws Exception {
        Map<String, Object> female = map("gender", "female", "count_*", 2L, "sum_kids", 3L, "min_age", 28L, "max_age", 32L, "avg_age", 30D);
        Map<String, Object> male = map("gender", "male", "count_*", 1L, "sum_kids", 3L, "min_age", 42L, "max_age", 42L, "avg_age", 42D);
        testResult(db, "CALL apoc.nodes.group(" +
            "['Person'],['gender'],[" +
              "{`*`:'count', kids:'sum', age:['min', 'max', 'avg'], gender:'collect'}," +
              "{`*`:'count', since:['min', 'max']}" +
            "])",
                (result) -> {
                    assertTrue(result.hasNext());
                    Map<String, Object> row = result.next();
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(1,nodes.size());
                    Node node = nodes.get(0);
                    String[] keys = {"count_*", "gender", "sum_kids", "min_age", "max_age", "avg_age"};
                    assertEquals(node.getProperty("gender").equals("female") ?
                      female : male, node.getProperties(keys));
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(1,rels.size());
                    Relationship rel = rels.get(0);
                    assertEquals(2L,rel.getProperty("count_*"));
                    assertEquals(2017L, rel.getProperty("min_since"));
                    assertEquals(2018L, rel.getProperty("max_since"));
                    assertEquals("KNOWS",rel.getType().name());
                    node = rel.getOtherNode(node);
                    assertEquals(node.getProperty("gender").equals("female") ?
                      female : male, node.getProperties(keys));
                    assertTrue(result.hasNext());

                    row = result.next();
                    nodes = (List<Node>) row.get("nodes");
                    assertEquals(1,nodes.size());
                    node = nodes.get(0);
                    assertEquals(node.getProperty("gender").equals("female") ?
                      female : male, node.getProperties(keys));

                    rels = (List<Relationship>) row.get("relationships");
                    assertEquals(0,rels.size());
                });
    }
}
