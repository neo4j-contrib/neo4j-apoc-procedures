package apoc.monitor;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;


public class LockProcedureTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Locks.class);
    }

    @Test
    @Ignore("information no longer available from Neo4j")
    public void testGetLockInfo() {
        testCall(db, "CALL apoc.monitor.locks(1000)", (row) -> {
            assertEquals("Showing contended locks where threads have waited for at least 1000 ms.", row.get("info"));
            assertEquals(1L, (long) row.get("contendedLockCount"));
            assertEquals(1L, (long) row.get("lockCount"));
            assertEquals(-1L, (long) row.get("advertedDeadLocks"));
        });
    }

}
