package apoc.monitor;

import apoc.util.TestUtil;
import org.junit.Test;

import static org.junit.Assert.*;

import static apoc.util.TestUtil.testCall;

public class IdProcedureTest extends MonitorTestCase {

    @Override
    Class procedureClass() {
        return Id.class;
    }

    @Test
    public void testGetNodeIdsInUse() {
        createData();
        testCall(db, "CALL apoc.monitor.ids()", (row) -> {
            long nodeIds = (long) row.get("nodeIds");
            long relIds = (long) row.get("relIds");
            long propIds = (long) row.get("propIds");
            long relTypeIds = (long) row.get("relTypeIds");
            assertEquals(6L, nodeIds);
            assertEquals(2L, relIds);
            assertEquals(1L, propIds);
            assertEquals(2L, relTypeIds);
        });
    }

    private void createData() {
        testCall(db, "CREATE (n)", (row) -> {});
        testCall(db, "CREATE (n)-[:REL_TYPE1]->(n2)", (row) -> {});
        testCall(db, "CREATE (n)-[:REL_TYPE2]->(n2)", (row) -> {});
        testCall(db, "CREATE (n) SET n.key = 123", (row) -> {});
    }

}
