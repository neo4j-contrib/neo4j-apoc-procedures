package apoc.monitor;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;


public class LockProcedureTest {
    private static GraphDatabaseService db;


    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Locks.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }


    @Test
    public void testGetLockInfo() {
        testCall(db, "CALL apoc.monitor.locks(1000)", (row) -> {
            assertEquals("Showing contended locks where threads have waited for at least 1000 ms.", row.get("info"));
            assertEquals(1L, (long) row.get("contendedLockCount"));
            assertEquals(1L, (long) row.get("lockCount"));
            assertEquals(-1L, (long) row.get("advertedDeadLocks"));
        });
    }

}
