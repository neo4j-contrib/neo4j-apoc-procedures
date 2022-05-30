package apoc.monitor;

import apoc.date.Date;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;

import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.text.SimpleDateFormat;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.*;

public class KernelProcedureTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Kernel.class);
    }

    private SimpleDateFormat format = new SimpleDateFormat(Date.DEFAULT_FORMAT);

    @AfterAll
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testGetKernelInfo() {
        long now = System.currentTimeMillis();
        testCall(db, "CALL apoc.monitor.kernel()", (row) -> {
            try {
                String startTime = (String) row.get("kernelStartTime");
                assertEquals("neo4j", row.get("databaseName"));
                assertTrue(format.parse(startTime).getTime() < now);
                assertNotNull( row.get( "kernelVersion" ) );
                assertNotNull(row.get("storeLogVersion"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
