package apoc.gephi;

import apoc.coll.Coll;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;
import java.net.ConnectException;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 29.05.16
 */
public class GephiTest {

    private static GraphDatabaseService db;
    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Gephi.class);
        db.execute("CREATE (:Foo {name:'Foo'})-[:KNOWS{weight:7.2,foo:'foo',bar:3.0,directed:'error',label:'foo'}]->(:Bar {name:'Bar'})").close();
    }
    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }
    @Test
    public void testAdd() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p) yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
        }, java.io.FileNotFoundException.class, java.net.ConnectException.class, IOException.class);
    }
    @Test
    public void testWeightParameter() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'weight') yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
        }, java.io.FileNotFoundException.class, java.net.ConnectException.class, IOException.class);
    }
    @Test
    public void testWrongWeightParameter() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'test') yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
        }, java.io.FileNotFoundException.class, java.net.ConnectException.class, IOException.class);
    }

    public void testRightExportParameter() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'weight',['foo']) yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
        }, java.io.FileNotFoundException.class, java.net.ConnectException.class, IOException.class);
    }

    public void testWrongExportParameter() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'weight',['faa','fee']) yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
        }, java.io.FileNotFoundException.class, java.net.ConnectException.class, IOException.class);
    }

    public void reservedExportParameter() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "MATCH p = (:Foo)-->() WITH p CALL apoc.gephi.add(null,'workspace1',p,'weight',['directed','label']) yield nodes, relationships, format return *", r -> {
                assertEquals(2L, r.get("nodes"));
                assertEquals(1L, r.get("relationships"));
                assertEquals("gephi", r.get("format"));
            });
        }, java.io.FileNotFoundException.class, java.net.ConnectException.class, IOException.class);
    }
}
