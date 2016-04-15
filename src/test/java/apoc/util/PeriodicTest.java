package apoc.util;

import apoc.periodic.Periodic;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class PeriodicTest {

    private GraphDatabaseService db;
    @Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Periodic.class);
    }
    @After public void tearDown() {
        db.shutdown();
    }

    @Test public void testSubmitStatement() throws Exception {
        testCall(db, "CALL apoc.periodic.submit('foo','create (:Foo)')",
                (row) -> {
                    assertEquals("foo", row.get("name"));
                    assertEquals(false, row.get("done"));
                    assertEquals(false, row.get("cancelled"));
                    assertEquals(0L, row.get("delay"));
                    assertEquals(0L, row.get("rate"));
                });
        testCall(db, "CALL apoc.periodic.list", (r) -> {
            assertEquals("foo", r.get("name"));
            assertEquals(false, r.get("done"));
        });
        Thread.sleep(1000);
        assertEquals(1L,db.execute("MATCH (:Foo) RETURN count(*) as c").columnAs("c").next());
        testCall(db, "CALL apoc.periodic.list", (r) -> assertEquals(true,r.get("done")));
    }

    public static final long RUNDONW_COUNT = 1000;
    public static final int BATCH_SIZE = 399;

    @Test
    public void testRunDown() throws Exception {
        db.execute("UNWIND range(1,{count}) as id CREATE (n:Person {id:id})",map("count",RUNDONW_COUNT)).close();

        String query = "MATCH (p:Person) WHERE NOT p:Processed WITH p LIMIT {limit} SET p:Processed RETURN count(*)";

        testCall(db,"CALL apoc.periodic.commit({query},{params})", map("query",query,"params",map("limit",BATCH_SIZE)), r -> {
            assertEquals((long)Math.ceil((double)RUNDONW_COUNT/BATCH_SIZE), r.get("executions"));
            assertEquals(RUNDONW_COUNT, r.get("updates"));
        });

        long count = db.execute("MATCH (p:Processed) return count(*) as c").<Long>columnAs("c").next();
        assertEquals(RUNDONW_COUNT,count);

    }
}
