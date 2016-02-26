package apoc.util;

import java.util.*;
import java.util.stream.*;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import org.junit.*;
import static org.junit.Assert.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.proc.Procedures;

public class UtilTest {

    private GraphDatabaseService db;
	@Before public void setUp() throws Exception {
	    db = new TestGraphDatabaseFactory().newImpermanentDatabase();
	    ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(Util.class);
	}
    @After public void tearDown() {
	    db.shutdown();
    }
    @Test public void testIN() throws Exception {
        Result res = db.execute("CALL apoc.util.IN(1,[1,2,3])");

        assertTrue(res.hasNext());
        Map<String,Object> row = res.next();
        assertEquals(true, row.get("value"));
        assertFalse(res.hasNext());
    }
    @Test public void testSort() throws Exception {
        Result res = db.execute("CALL apoc.util.sort([3,2,1])");

        assertTrue(res.hasNext());
        Map<String,Object> row = res.next();
        assertEquals(Arrays.asList(1L,2L,3L), row.get("value"));
        assertFalse(res.hasNext());
    }
    @Test public void testSortNodes() throws Exception {
        try (Transaction tx = db.beginTx()) {
	        Result res = db.execute("CREATE (n {name:'foo'}),(m {name:'bar'}) WITH n,m CALL apoc.util.sortNodes([n,m], 'name') YIELD value RETURN value");
	
	        assertTrue(res.hasNext());
	        Map<String,Object> row = res.next();
			List<Node> nodes = (List<Node>)row.get("value");
	        assertEquals("bar",nodes.get(0).getProperty("name"));
	        assertEquals("foo",nodes.get(1).getProperty("name"));
	        assertFalse(res.hasNext());
        }
    }
}