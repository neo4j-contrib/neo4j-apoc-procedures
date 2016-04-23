package apoc.util;

import apoc.convert.Json;
import apoc.load.LoadJson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.URL;
import java.util.Map;

import static apoc.util.TestUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
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
}
