package apoc.cache;

import apoc.ApocConfiguration;
import apoc.cypher.Cypher;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 22.05.16
 */
public class StaticTest {
    public static final String VALUE = "testValue";
    private GraphDatabaseService db;

    @Before
    public  void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("apoc.static.test",VALUE)
                .setConfig("apoc.static.all.test",VALUE)
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Static.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testGetAllFromConfig() throws Exception {
        TestUtil.testCall(db, "call apoc.static.getAll('all')", r -> assertEquals(map("test",VALUE),r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('all.test2',42)", r -> assertEquals(null,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.getAll('all')", r -> assertEquals(map("test",VALUE,"test2",42L),r.get("value")));
        TestUtil.testCall(db, "call apoc.static.getAll('')", r -> assertEquals(map("test",VALUE,"all.test",VALUE,"all.test2",42L),r.get("value")));
    }
    @Test
    public void testListFromConfig() throws Exception {
        TestUtil.testCall(db, "call apoc.static.set('all.test2',42)", r -> assertEquals(null,r.get("value")));
        TestUtil.testResult(db, "call apoc.static.list('all') yield key, value return * order by key", r -> {
            assertEquals(map("key","test","value",VALUE),r.next());
            assertEquals(map("key","test2","value",42L),r.next());
            assertEquals(false, r.hasNext());
        });
        TestUtil.testResult(db, "call apoc.static.list('') yield key, value return * order by key", r -> {
            assertEquals(map("key","all.test","value",VALUE),r.next());
            assertEquals(map("key","all.test2","value",42L),r.next());
            assertEquals(map("key","test","value",VALUE),r.next());
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testOverrideConfig() throws Exception {
        TestUtil.testCall(db, "call apoc.static.get('test')", r -> assertEquals(VALUE,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('test',42)", r -> assertEquals(VALUE,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.get('test')", r -> assertEquals(42L,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('test',null)", r -> assertEquals(42L,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.get('test')", r -> assertEquals(VALUE,r.get("value")));
    }

    @Test
    public void testSet() throws Exception {
        TestUtil.testCall(db, "call apoc.static.get('test2')", r -> assertEquals(null,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('test2',42)", r -> assertEquals(null,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.get('test2')", r -> assertEquals(42L,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('test2',null)", r -> assertEquals(42L,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.get('test2')", r -> assertEquals(null,r.get("value")));
    }
}
