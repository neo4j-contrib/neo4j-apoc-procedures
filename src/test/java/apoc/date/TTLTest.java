package apoc.date;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 21.05.16
 */
public class TTLTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("apoc.ttl.schedule","5")
                .setConfig("apoc.ttl.enabled","true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Date.class);
        db.execute("CREATE (n:Foo:TTL) SET n.ttl = timestamp() + 100").close();
        db.execute("CREATE (n:Bar) WITH n CALL apoc.date.expireIn(n,500,'ms') RETURN count(*)").close();
        testNodes(1,1);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExpire() throws Exception {
        Thread.sleep(10*1000);
        testNodes(0,0);
    }

    private static void testNodes(int foo, int bar) {
        try (Transaction tx=db.beginTx()) {
            assertEquals(foo, Iterators.count(db.findNodes(Label.label("Foo"))));
            assertEquals(bar, Iterators.count(db.findNodes(Label.label("Bar"))));
            assertEquals(foo + bar, Iterators.count(db.findNodes(Label.label("TTL"))));
            tx.success();
        }
    }
}
