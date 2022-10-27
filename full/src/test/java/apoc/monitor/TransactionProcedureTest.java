package apoc.monitor;

import apoc.cypher.CypherInitializer;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.concurrent.atomic.AtomicLong;

import static apoc.util.CypherInitializerUtil.getInitializer;
import static apoc.util.TestUtil.testCall;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

public class TransactionProcedureTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Transaction.class);
        // we need to wait until every CypherInitializer transaction is finished to make sure tests are not flaky
        waitForInitializerBeingFinished(db);
    }

    @Test
    public void testGetStoreInfo() {
        AtomicLong peakTx = new AtomicLong();
        AtomicLong lastTxId = new AtomicLong();
        AtomicLong totalOpenedTx = new AtomicLong();
        AtomicLong totalTx = new AtomicLong();
        testCall(db, "CALL apoc.monitor.tx()", (row) -> {
            assertEquals(0l, row.get("rolledBackTx"));
            peakTx.set((long) row.get("peakTx"));
            assertThat(peakTx.get(), isOneOf(1l, 2l));
            assertEquals(3l, lastTxId.addAndGet((long) row.get("lastTxId")));
            assertEquals(1l, row.get("currentOpenedTx"));
            assertEquals(4l, totalOpenedTx.addAndGet((long) row.get("totalOpenedTx")));
            assertEquals(3l, totalTx.addAndGet((long )row.get("totalTx")));
        });

        db.executeTransactionally("create ()");
        testCall(db, "CALL apoc.monitor.tx()", (row) -> {
            assertEquals(0l, row.get("rolledBackTx"));
            assertEquals(peakTx.get(), row.get("peakTx"));
            assertEquals(lastTxId.incrementAndGet(), row.get("lastTxId"));
            assertEquals(1l, row.get("currentOpenedTx"));
            assertEquals(totalOpenedTx.addAndGet(2L), row.get("totalOpenedTx"));
            assertEquals(totalTx.addAndGet(2L), row.get("totalTx"));
        });
    }

    // equivalent to CypherInitializerTest.waitForInitializerBeingFinished
    private static void waitForInitializerBeingFinished(DbmsRule dbmsRule) {
        CypherInitializer initializer = getInitializer(dbmsRule.databaseName(), dbmsRule, CypherInitializer.class);
        while (!initializer.isFinished()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
