package apoc.convert;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ConvertJsonTest {

    private GraphDatabaseService db;
	@Before public void setUp() throws Exception {
	    db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Json.class);
    }

    @After public void tearDown() {
	    db.shutdown();
    }

    @Test public void testToJsonList() throws Exception {
	    testCall(db, "RETURN apoc.convert.toJson([1,2,3]) as value",
	             (row) -> assertEquals("[1,2,3]", row.get("value")) );
    }
    @Test public void testToJsonMap() throws Exception {
	    testCall(db, "RETURN apoc.convert.toJson({a:42,b:\"foo\",c:[1,2,3]}) as value",
	             (row) -> assertEquals("{\"a\":42,\"b\":\"foo\",\"c\":[1,2,3]}", row.get("value")) );
    }
    @Test public void testFromJsonList() throws Exception {
	    testCall(db, "RETURN apoc.convert.fromJsonList('[1,2,3]') as value",
	             (row) -> assertEquals(asList(1,2,3), row.get("value")) );
	    testCall(db, "RETURN apoc.convert.fromJsonList('{\"foo\":[1,2,3]}','$.foo') as value",
	             (row) -> assertEquals(asList(1,2,3), row.get("value")) );
    }
    @Test public void testFromJsonMap() throws Exception {
	    testCall(db, "RETURN apoc.convert.fromJsonMap('{\"a\":42,\"b\":\"foo\",\"c\":[1,2,3]}')  as value",
	             (row) -> {
		           Map value = (Map)row.get("value");
		           assertEquals(42, value.get("a"));
		           assertEquals("foo", value.get("b"));
		           assertEquals(asList(1,2,3), value.get("c"));
		         });
    }

    @Test public void testSetJsonProperty() throws Exception {
        testCall(db, "CREATE (n) WITH n CALL apoc.convert.setJsonProperty(n, 'json', [1,2,3]) RETURN n",
                (row) -> assertEquals("[1,2,3]", ((Node)row.get("n")).getProperty("json")));
    }

    @Test public void testGetJsonProperty() throws Exception {
        testCall(db, "CREATE (n {json:'[1,2,3]'}) RETURN apoc.convert.getJsonProperty(n, 'json') AS value",
                (row) -> assertEquals(asList(1,2,3), row.get("value")) );
        testCall(db, "CREATE (n {json:'{\"foo\":[1,2,3]}'}) RETURN apoc.convert.getJsonProperty(n, 'json','$.foo') AS value",
                (row) -> assertEquals(asList(1,2,3), row.get("value")) );
    }
    @Test public void testGetJsonPropertyMap() throws Exception {
        testCall(db, "CREATE (n {json:'{a:[1,2,3]}'}) RETURN apoc.convert.getJsonProperty(n, 'json') as value",
                (row) -> assertEquals(map("a",asList(1,2,3)), row.get("value")) );
        testCall(db, "CREATE (n {json:'{a:[1,2,3]}'}) RETURN apoc.convert.getJsonProperty(n, 'json','$.a') as value",
                (row) -> assertEquals(asList(1,2,3), row.get("value")) );
    }

    @Test public void testToTree() throws Exception {
        testCall(db, "CREATE p1=(m:Movie {title:'M'})<-[:ACTED_IN {role:'R1'}]-(:Actor {name:'A1'}), " +
                " p2 = (m)<-[:ACTED_IN  {role:'R2'}]-(:Actor {name:'A2'}) WITH [p1,p2] as paths " +
                " CALL apoc.convert.toTree(paths) YIELD value RETURN value",
                (row) -> {
                    Map root = (Map) row.get("value");
                    System.out.println("root = " + root);
                    assertEquals("Movie", root.get("_type"));
                    assertEquals("M", root.get("title"));
                    List<Map> actors = (List<Map>) root.get("acted_in");
                    assertEquals("Actor", actors.get(0).get("_type"));
                    assertEquals(true, actors.get(0).get("name").toString().matches("A[12]"));
                    assertEquals(true, actors.get(0).get("acted_in.role").toString().matches("R[12]"));
                });
    }
    @Test public void testToTreeLeafNodes() throws Exception {
        String createStatement = "CREATE\n" +
                "  (c1:Category {name: 'PC'}),\n" +
                "    (c1)-[:subcategory {id:1}]->(c2:Category {name: 'Parts'}),\n" +
                "      (c2)-[:subcategory {id:2}]->(c3:Category {name: 'CPU'})";
        db.execute(createStatement).close();

        String call = "MATCH p=(n:Category)-[:subcategory*]->(m)\n" +
                "WHERE NOT (m)-[:subcategory]->() AND NOT ()-[:subcategory]->(n)\n" +
                "WITH COLLECT(p) AS ps\n" +
                "CALL apoc.convert.toTree(ps) yield value\n" +
                "RETURN value;";
        testCall(db, call,
                (row) -> {
                    Map root = (Map) row.get("value");
                    System.out.println("root = " + root);

                    assertEquals("Category", root.get("_type"));
                    assertEquals("PC", root.get("name"));
                    List<Map> parts = (List<Map>) root.get("subcategory");
                    assertEquals(1,parts.size());
                    Map pcParts = parts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertEquals("Parts", pcParts.get("name"));
                    List<Map> subParts = (List<Map>)pcParts.get("subcategory");
                    Map cpu = subParts.get(0);
                    assertEquals(1,subParts.size());
                    assertEquals("Category", cpu.get("_type"));
                    assertEquals("CPU", cpu.get("name"));
                });
    }
}
