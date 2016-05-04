package apoc.map;

import apoc.coll.Coll;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 04.05.16
 */
public class MapsTest {

    private GraphDatabaseService db;
    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Maps.class);
    }
    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testFromPairs() throws Exception {
        TestUtil.testCall(db, "CALL apoc.map.fromPairs([['a',1],['b',false]])", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testFromLists() throws Exception {
        TestUtil.testCall(db, "CALL apoc.map.fromLists(['a','b'],[1,false])", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testSetKey() throws Exception {
        TestUtil.testCall(db, "CALL apoc.map.setKey({a:1},'a',2)", (r) -> {
            assertEquals(map("a",2L),r.get("value"));
        });
    }

    @Test
    public void testClean() throws Exception {
        TestUtil.testCall(db, "CALL apoc.map.clean({a:1,b:'',c:null,x:1234,z:false},['x'],['',false])", (r) -> {
            assertEquals(map("a",1L),r.get("value"));
        });
    }
}
