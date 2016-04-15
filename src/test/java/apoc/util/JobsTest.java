package apoc.util;

import apoc.jobs.Jobs;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;

import static apoc.util.TestUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JobsTest {

    private GraphDatabaseService db;
    @Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Jobs.class);
    }
    @After public void tearDown() {
        db.shutdown();
    }

    @Ignore("AccessMode Broken in GDB")
    @Test public void testSubmitStatement() throws Exception {
        testCall(db, "CALL apoc.jobs.submit('foo','create (:Foo)')",
                (row) -> {
                    assertEquals("foo", row.get("name"));
                    assertEquals(false, row.get("done"));
                    assertEquals(false, row.get("cancelled"));
                    assertEquals(0L, row.get("delay"));
                    assertEquals(0L, row.get("rate"));
                });
        Thread.sleep(100);
        assertEquals(1,db.execute("MATCH (:Foo) RETURN count(*) as c").columnAs("c").next());
    }

    public static final long RUNDONW_COUNT = 1000;
    @Test
    public void testRunDown() throws Exception {
        db.execute("UNWIND range(1," + RUNDONW_COUNT + ") as id CREATE (n:Person {id:id})").close();

        String query = "MATCH (p:Person) WHERE NOT p:Processed WITH p LIMIT 99 SET p:Processed RETURN count(*)";

        testCall(db,"CALL apoc.jobs.rundown({query},null)", map("query",query), r -> {
            assertEquals((long)Math.ceil((double)RUNDONW_COUNT/99), r.get("executions"));
            assertEquals(RUNDONW_COUNT, r.get("updates"));
        });

        long count = db.execute("MATCH (p:Processed) return count(*) as c").<Long>columnAs("c").next();
        assertEquals(RUNDONW_COUNT,count);

    }
}
