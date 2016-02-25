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
    @Test public void testIN() throws Exception {
	    GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
	    ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(Util.class);

        Result res = db.execute("CALL apoc.util.IN(1,[1,2,3])");

        assertTrue(res.hasNext());
        Map<String,Object> row = res.next();
        assertEquals(true, row.get("value"));
        assertFalse(res.hasNext());
    }
    @Test public void testSort() throws Exception {
	    GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
	    ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(Util.class);

        Result res = db.execute("CALL apoc.util.sort([3,2,1])");

        assertTrue(res.hasNext());
        Map<String,Object> row = res.next();
        assertEquals(Arrays.asList(1L,2L,3L), row.get("value"));
        assertFalse(res.hasNext());
    }
}