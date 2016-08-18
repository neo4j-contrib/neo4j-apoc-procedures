package apoc.load;

import java.net.URL;
import java.util.Map;

import apoc.load.LoadJson;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.asList;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class LoadJsonTest {

    private GraphDatabaseService db;
	@Before public void setUp() throws Exception {
	    db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, LoadJson.class);
    }

    @After public void tearDown() {
	    db.shutdown();
    }

    @Test public void testLoadJson() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1,2,3)), row.get("value"));
                });
    }
    @Test @Ignore public void testLoadJsonGraphCommons() throws Exception {
		String url = "https://graphcommons.com/graphs/8da5327d-7829-4dfe-b60b-4c0bda956b2a.json";
		testCall(db, "CALL apoc.load.json({url})",map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    Map value = (Map)row.get("value");
                    assertEquals(true, value.containsKey("users"));
                    assertEquals(true, value.containsKey("nodes"));
                });
    }
}
