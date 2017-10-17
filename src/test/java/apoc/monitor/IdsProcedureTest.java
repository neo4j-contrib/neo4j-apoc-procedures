package apoc.monitor;

import org.junit.Test;

import static org.junit.Assert.*;

import static apoc.util.TestUtil.testCall;

public class IdsProcedureTest extends MonitorTestCase {

    @Override
    Class procedureClass() {
        return Ids.class;
    }

    @Test
    public void testGetNodeIdsInUse() {
        createData();
        testCall(db, "CALL apoc.monitor.ids()", (row) -> {
            long nodeIds = (long) row.get("nodeIds");
            long relIds = (long) row.get("relIds");
            long propIds = (long) row.get("propIds");
            long relTypeIds = (long) row.get("relTypeIds");
            assertEquals(true, nodeIds >= 6L);
            // no longer correct, due to batch id-allocation:
            // assertEquals(6L, nodeIds);
            // assertEquals(2L, relIds);
            // assertEquals(1L, propIds);
            assertEquals(true, relIds >= 2L);
            assertEquals(true, propIds >= 1L);
            assertEquals(2L, relTypeIds);
        });
    }

    private void createData() {
        db.execute("CREATE (n)");
        db.execute("CREATE (n)-[:REL_TYPE1]->(n2)");
        db.execute("CREATE (n)-[:REL_TYPE2]->(n2)");
        db.execute("CREATE (n) SET n.key = 123");
    }

}
