package apoc.monitor;

import org.junit.Test;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.*;

public class StoreInfoProcedureTest extends MonitorTestCase {

    @Override
    Class procedureClass() {
        return Store.class;
    }

    @Test
    public void testGetStoreInfo() {
        testCall(db, "CALL apoc.monitor.store()", (row) -> {
            assertNotNull(row.get("logSize"));
            assertNotNull(row.get("stringStoreSize"));
            assertNotNull(row.get("arrayStoreSize"));
            assertNotNull(row.get("nodeStoreSize"));
            assertNotNull(row.get("relStoreSize"));
            assertNotNull(row.get("propStoreSize"));
            assertNotNull(row.get("totalStoreSize"));
        });
    }
}
