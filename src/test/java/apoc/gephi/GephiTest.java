package apoc.gephi;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.serverListening;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * @author mh
 * @since 29.05.16
 */
public class GephiTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        assumeTrue(serverListening("localhost", 8080));
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Gephi.class);
        db.execute("CREATE (:Foo {name:'Foo'})-[:KNOWS{weight:7.2,foo:'foo',bar:3.0,directed:'error',label:'foo'}]->(:Bar {name:'Bar'})").close();
    }

    @AfterClass
    public static void tearDown() {
        if (db!=null) {
            db.shutdown();
        }
    }

    @Test
    public void testAdd() throws Exception {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p) yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
    }
    @Test
    public void testWeightParameter() throws Exception {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'weight') yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
    }

    @Test
    public void testWrongWeightParameter() throws Exception {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'test') yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
    }

    @Test
    public void testRightExportParameter() throws Exception {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'weight',['foo']) yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
    }

    @Test
    public void testWrongExportParameter() throws Exception {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'weight',['faa','fee']) yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
    }

    @Test
    public void reservedExportParameter() throws Exception {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'weight',['directed','label']) yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
    }
}
