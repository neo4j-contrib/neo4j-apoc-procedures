package apoc.lock;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LockTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.lock_acquisition_timeout, Duration.ofSeconds(1));

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Lock.class);
    }

    @Test
    public void shouldReadLockBlockAWrite() throws Exception {

        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            final Node n = Iterators.single(tx.execute("match (n) CALL apoc.lock.read.nodes([n]) return n").columnAs("n"));
            assertEquals(n, node);

            final Thread thread = new Thread(() -> {
                System.out.println(Instant.now().toString() + " pre-delete");
                try {
                    db.executeTransactionally("match (n) delete n", Collections.emptyMap(), result -> result.resultAsString());
                    fail("expecting lock timeout");
                } catch (LockAcquisitionTimeoutException e) {
                }
                System.out.println(Instant.now().toString() + " delete");

            });
            thread.start();
            thread.join(5000L);

            // the blocked thread didn't do any work, so we still have nodes
            long count = Iterators.count(tx.execute("match (n) return n").columnAs("n"));
            assertEquals(1, count);

            tx.commit();
        }

    }
}
