package apoc.monitor;

import apoc.date.Date;
import org.junit.Test;

import java.text.SimpleDateFormat;

import static org.junit.Assert.*;
import static apoc.util.TestUtil.testCall;

public class KernelProcedureTest extends MonitorTestCase {

    private SimpleDateFormat format = new SimpleDateFormat(Date.DEFAULT_FORMAT);

    @Override
    Class procedureClass() {
        return Kernel.class;
    }

    @Test
    public void testGetKernelInfo() {
        long now = System.currentTimeMillis();
        testCall(db, "CALL apoc.monitor.kernel()", (row) -> {
            try {
                String startTime = (String) row.get("kernelStartTime");
                String kernelVersion = String.valueOf(row.get("kernelVersion"));
                assertEquals("impermanent-db", String.valueOf(row.get("databaseName")));
                assertTrue(format.parse(startTime).getTime() < now);
                assertTrue(kernelVersion.contains("3."));
                assertEquals(0, (long) row.get("storeLogVersion"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
