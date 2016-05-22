package apoc.cache;

import apoc.cypher.Cypher;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 22.05.16
 */
public class StaticTest {
    public static final String VALUE = "testValue";
    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("apoc.static.test",VALUE)
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Static.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testGetFromConfig() throws Exception {
        TestUtil.testCall(db, "call apoc.static.get('test')", r -> assertEquals(VALUE,r.get("value")));
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
