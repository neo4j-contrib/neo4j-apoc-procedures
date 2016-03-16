package apoc.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UtilTest {

    private GraphDatabaseService db;
    @Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Util.class);
    }
    @After public void tearDown() {
        db.shutdown();
    }

    @Test public void testIN() throws Exception {
        testCall(db, "CALL apoc.util.IN(1,[1,2,3])",
                (row) -> assertEquals(true, row.get("value")));
    }
    @Test public void testSort() throws Exception {
        testCall(db, "CALL apoc.util.sort([3,2,1])",
                (row) -> assertEquals(asList(1L,2L,3L), row.get("value")));
    }
    @Test public void testSortNodes() throws Exception {
        testCall(db,
            "CREATE (n {name:'foo'}),(m {name:'bar'}) WITH n,m CALL apoc.util.sortNodes([n,m], 'name') YIELD value RETURN value",
            (row) -> {
                List<Node> nodes = (List<Node>) row.get("value");
                assertEquals("bar", nodes.get(0).getProperty("name"));
                assertEquals("foo", nodes.get(1).getProperty("name"));
            });
    }
}
