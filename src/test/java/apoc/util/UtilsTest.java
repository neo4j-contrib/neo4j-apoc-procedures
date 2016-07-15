package apoc.util;

import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 26.05.16
 */
public class UtilsTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Utils.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testSha1() throws Exception {
        TestUtil.testCall(db, "call apoc.util.sha1(['ABC'])", r -> assertEquals("3c01bdbb26f358bab27f267924aa2c9a03fcfdb8", r.get("value")));
    }

    @Test
    public void testMd5() throws Exception {
        TestUtil.testCall(db, "call apoc.util.md5(['ABC'])", r -> assertEquals("902fbdd2b1df0c4f70b4a5d23525e932", r.get("value")));
    }

    @Test
    public void testSleep() {
        String cypherSleep = "call apoc.util.sleep({duration})";
        TestUtil.testCall(db, cypherSleep, MapUtil.map("duration", 0l), r -> {
        });  // force building query plan

        long duration = 300;
        TestUtil.assertDuration(Matchers.greaterThanOrEqualTo(duration), () -> {
            TestUtil.testCall(db, cypherSleep, MapUtil.map("duration", duration), r -> { });
            return null;
        });
    }

    @Test
    public void testSleepWithTerminate() {
        String cypherSleep = "call apoc.util.sleep({duration})";
        TestUtil.testCall(db, cypherSleep, MapUtil.map("duration", 0l), r -> {
        });  // force building query plan

        long duration = 300;
        TestUtil.assertDuration(Matchers.lessThan(duration), () -> {
            final Transaction[] tx = new Transaction[1];

            Future future = Executors.newSingleThreadScheduledExecutor().submit( () -> {
                tx[0] = db.beginTx();
                try {
                    Result result = db.execute(cypherSleep, MapUtil.map("duration", 10000));
                    tx[0].success();
                    return result;
                } finally {
                    tx[0].close();
                }
            });

            sleepUntil( dummy -> tx[0]!=null );
            tx[0].terminate();
            try {
                future.get();
            } catch (InterruptedException|ExecutionException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private void sleepUntil(Predicate<Void> predicate) {
        while (!predicate.test(null)) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
