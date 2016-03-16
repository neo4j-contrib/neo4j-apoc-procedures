package apoc.util;

import apoc.jobs.Jobs;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;

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
}
