package apoc.monitor;

import org.junit.Test;

import static org.junit.Assert.*;
import static apoc.util.TestUtil.testCall;

public class KernelInfoProcedureTest extends MonitorTestCase {

    @Override
    Class procedureClass() {
        return KernelInfo.class;
    }

    @Test
    public void testGetKernelInfo() {
        long now = System.currentTimeMillis();
        testCall(db, "CALL apoc.monitor.kernelInfo()", (row) -> {
            long startTime = (long) row.get("kernelStartTime");
            String kernelVersion = String.valueOf(row.get("kernelVersion"));
            assertEquals("impermanent-db", String.valueOf(row.get("databaseName")));
            assertTrue(startTime < now);
            assertTrue(kernelVersion.contains("3.0.0"));
            assertEquals(0, (long) row.get("storeLogVersion"));
        });
    }
}
