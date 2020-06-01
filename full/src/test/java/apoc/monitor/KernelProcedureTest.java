package apoc.monitor;

import apoc.date.Date;
import apoc.date.DateExpiry;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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

    @Test
    public void testGetKernelInfo() {
        long now = System.currentTimeMillis();
        testCall(db, "CALL apoc.monitor.kernel()", (row) -> {
            try {
                String startTime = (String) row.get("kernelStartTime");
                String kernelVersion = String.valueOf(row.get("kernelVersion"));
                assertEquals("neo4j", row.get("databaseName"));
                assertTrue(format.parse(startTime).getTime() < now);
                assertTrue(kernelVersion.contains("4."));
                assertNotNull(row.get("storeLogVersion"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
