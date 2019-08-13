package apoc.monitor;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertNotNull;

public class StoreInfoProcedureTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Store.class);
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
