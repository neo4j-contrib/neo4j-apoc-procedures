package apoc.convert;

import apoc.convert.Json;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
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
	    testCall(db, "CALL apoc.convert.toJson([1,2,3])",
	             (row) -> assertEquals("[1,2,3]", row.get("value")) );
    }
    @Test public void testToJsonMap() throws Exception {
	    testCall(db, "CALL apoc.convert.toJson({a:42,b:\"foo\",c:[1,2,3]})",
	             (row) -> assertEquals("{\"a\":42,\"b\":\"foo\",\"c\":[1,2,3]}", row.get("value")) );
    }
    @Test public void testFromJsonList() throws Exception {
	    testCall(db, "CALL apoc.convert.fromJsonList('[1,2,3]')",
	             (row) -> assertEquals(asList(1,2,3), row.get("value")) );
    }
    @Test public void testFromJsonMap() throws Exception {
	    testCall(db, "CALL apoc.convert.fromJsonMap('{\"a\":42,\"b\":\"foo\",\"c\":[1,2,3]}')",
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
        testCall(db, "CREATE (n {json:'[1,2,3]'}) WITH n CALL apoc.convert.getJsonProperty(n, 'json') YIELD value RETURN value",
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
}
