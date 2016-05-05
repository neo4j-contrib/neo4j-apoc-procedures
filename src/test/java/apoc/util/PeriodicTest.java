package apoc.util;

import apoc.periodic.Periodic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static apoc.util.TestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
        String callList = "CALL apoc.periodic.list";
        // force pre-caching the queryplan
        assertFalse(db.execute(callList).hasNext());

        testCall(db, "CALL apoc.periodic.submit('foo','create (:Foo)')",
                (row) -> {
                    assertEquals("foo", row.get("name"));
                    assertEquals(false, row.get("done"));
                    assertEquals(false, row.get("cancelled"));
                    assertEquals(0L, row.get("delay"));
                    assertEquals(0L, row.get("rate"));
                });
        testCall(db, callList, (r) -> {
            assertEquals("foo", r.get("name"));
            assertEquals(false, r.get("done"));
        });
        Thread.sleep(2000);
        ResourceIterator<Object> it = db.execute("MATCH (:Foo) RETURN count(*) as c").columnAs("c");
        assertEquals(1L, it.next());
        it.close();
        testCall(db, callList, (r) -> assertEquals(true,r.get("done")));
    }

    public static final long RUNDONW_COUNT = 1000;
    public static final int BATCH_SIZE = 399;

    @Test
    public void testRunDown() throws Exception {
        db.execute("UNWIND range(1,{count}) as id CREATE (n:Person {id:id})", MapUtil.map("count",RUNDONW_COUNT)).close();

        String query = "MATCH (p:Person) WHERE NOT p:Processed WITH p LIMIT {limit} SET p:Processed RETURN count(*)";

        testCall(db,"CALL apoc.periodic.commit({query},{params})", MapUtil.map("query",query,"params", MapUtil.map("limit",BATCH_SIZE)), r -> {
            assertEquals((long)Math.ceil((double)RUNDONW_COUNT/BATCH_SIZE), r.get("executions"));
            assertEquals(RUNDONW_COUNT, r.get("updates"));
        });

        ResourceIterator<Long> it = db.execute("MATCH (p:Processed) return count(*) as c").<Long>columnAs("c");
        long count = it.next();
        it.close();
        assertEquals(RUNDONW_COUNT,count);

    }

    @Test
    public void testRock_n_roll() throws Exception {
        // setup
        db.execute("UNWIND range(1,100) as x create (:Person{name:'Person_'+x})").close();

        // when&then
        testResult(db, "CALL apoc.periodic.rock_n_roll('match (p:Person) return p', 'MATCH (p) where p={p} SET p.lastname =p.name', 10)", result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(10l, row.get("batches"));
                    assertEquals(100l, row.get("total"));
                });
        // then
        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count" ,
                row -> assertEquals(100l, row.get("count"))
        );
    }

}
