package apoc.monitor;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import apoc.util.TestUtil;
import static org.junit.Assert.*;
import static apoc.util.TestUtil.testCall;


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
        testCall(db, "CALL apoc.monitor.lock(1000)", (row) -> {
            try {
                assertEquals("Showing contended locks where threads have waited for at least 1000 ms.", (String) row.get("info"));
                assertEquals(1l,(long) row.get("contendedLockCount"));
                assertEquals(1l,(long) row.get("lockCount"));
                assertEquals(-1l,(long) row.get("advertedDeadLocks"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

}
