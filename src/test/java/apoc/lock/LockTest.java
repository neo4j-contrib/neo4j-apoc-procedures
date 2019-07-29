package apoc.lock;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class LockTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Lock.class);
    }

    @Test
    public void shouldReadLockBlockAWrite() throws Exception {

        Node node;
        try (Transaction tx = db.beginTx()) {
            node = db.createNode();
            tx.success();
        }

        try (Transaction tx = db.beginTx()) {
            final Object n = Iterators.single(db.execute("match (n) CALL apoc.lock.read.nodes([n]) return n").columnAs("n"));
            assertEquals(n, node);

            final Thread thread = new Thread(() -> {
                db.execute("match (n) delete n");

            });
            thread.start();
            thread.join(TimeUnit.SECONDS.toMillis(1));

            // the blocked thread didn't do any work, so we still have nodes
            long count = Iterators.count(db.execute("match (n) return n").columnAs("n"));
            assertEquals(1, count);

            tx.success();
        }



    }
}
