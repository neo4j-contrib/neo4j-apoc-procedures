package apoc.monitor;

import apoc.ApocExtensionFactory;
import apoc.cypher.CypherInitializer;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;

import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static apoc.util.TestUtil.testCall;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class TransactionProcedureTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Transaction.class);
        // we need to wait until every CypherInitializer transaction is finished to make sure tests are not flaky
        waitForInitializerBeingFinished(db);
    }

    @AfterAll
    public void tearDown() {
        db.shutdown();
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
            final long totalOpenedTxBefore = totalOpenedTx.addAndGet((long) row.get("totalOpenedTx"));
            // totalOpenedTx can be either 4 or 5, depend on db.executeTransactionally("CALL dbms.components"...) placed in CypherInitializer
            assertTrue("Actual totalOpenedTx is " + totalOpenedTxBefore, List.of(4L, 5L).contains(totalOpenedTxBefore));
            assertEquals(totalOpenedTxBefore - 1L, totalTx.addAndGet((long )row.get("totalTx")));
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
    private void waitForInitializerBeingFinished(DbmsRule dbmsRule) {
        CypherInitializer initializer = getInitializer(dbmsRule.databaseName());
        while (!initializer.isFinished()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
    
    /**
     * get a reference to CypherInitializer for diagnosis. This needs to use reflection.
     * @return
     */
    private CypherInitializer getInitializer(String dbName) {
        var api = ((GraphDatabaseAPI) (db.getManagementService().database( dbName )));
        var apoc = api.getDependencyResolver().resolveDependency( ApocExtensionFactory.ApocLifecycle.class );
        var listeners = apoc.getRegisteredListeners();
        for ( AvailabilityListener listener : listeners )
        {
            if ( listener instanceof CypherInitializer )
            {
                return (CypherInitializer) listener;
            }
        }
        throw new IllegalStateException( "found no cypher initializer" );
    }
}
