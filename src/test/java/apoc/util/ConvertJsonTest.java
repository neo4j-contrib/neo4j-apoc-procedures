package apoc.util;

import apoc.convert.Json;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

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
}
