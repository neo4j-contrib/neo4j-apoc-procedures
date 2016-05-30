package apoc.gephi;

import apoc.coll.Coll;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

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
        db.execute("CREATE (f:Foo {name:'Foo'})-[:KNOWS]->(b:Bar {name:'Bar'})").close();
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
        }, ConnectException.class);
    }
}
