package apoc.monitor;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class TransactionProcedureTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Transaction.class);
    }

    @Test
    public void testGetStoreInfo() {
        testCall(db, "CALL apoc.monitor.tx()", (row) -> {
            assertEquals(0l, row.get("rolledBackTx"));
            assertEquals(1l, row.get("peakTx"));
            assertEquals(1l, row.get("lastTxId"));
            assertEquals(1l, row.get("currentOpenedTx"));
            assertEquals(2l, row.get("totalOpenedTx"));
            assertEquals(1l, row.get("totalTx"));
        });

        db.executeTransactionally("create ()");
        testCall(db, "CALL apoc.monitor.tx()", (row) -> {
            assertEquals(0l, row.get("rolledBackTx"));
            assertEquals(1l, row.get("peakTx"));
            assertEquals(2l, row.get("lastTxId"));
            assertEquals(1l, row.get("currentOpenedTx"));
            assertEquals(4l, row.get("totalOpenedTx"));
            assertEquals(3l, row.get("totalTx"));
        });
    }
}
