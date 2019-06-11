package apoc.util;

import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import static apoc.util.TestUtil.testCallEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        TestUtil.testCall(db, "RETURN apoc.util.sha1(['ABC']) AS value", r -> assertEquals("3c01bdbb26f358bab27f267924aa2c9a03fcfdb8", r.get("value")));
    }

    @Test
    public void testMd5() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.util.md5(['ABC']) AS value", r -> assertEquals("902fbdd2b1df0c4f70b4a5d23525e932", r.get("value")));
    }

    @Test
    public void testCompress() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.util.compress('abc') AS value",
                r -> assertEquals(Arrays.asList(97L, 98L, 99L), r.get("value")));
    }

    @Test
    public void testDecompress() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.util.decompress([97,98,99]) AS value",
                r -> assertEquals("abc", r.get("value")));
    }

    @Test
    public void testCompressWithCharSet() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.util.compress('ôo',{charSet:'ISO-8859-1'}) AS value",
                r -> assertEquals( Arrays.asList(244L, 111L), r.get( "value" ) ));
    }

    @Test
    public void testDecompressWithCharSet() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.util.decompress([244,111], {charSet:'ISO-8859-1'}) AS value",
                r -> assertEquals("ôo", r.get("value")));
    }

    @Test
    public void testCompressDecompress() throws Exception {
        TestUtil.testCall(db, "WITH apoc.util.compress('ôo', {charSet:'ISO-8859-1'}) AS iso,apoc.util.compress('ôo', {charSet:'UTF-16'}) AS utf" +
                        " RETURN iso, utf, apoc.util.decompress(iso, {charSet:'ISO-8859-1'}) AS dIso,  apoc.util.decompress(utf, {charSet:'UTF-16'}) AS dUtf",
                r ->
                {
                    assertEquals( Arrays.asList(244L, 111L), r.get( "iso" ) );
                    assertEquals( Arrays.asList(254L, 255L, 0L, 244L, 0L, 111L), r.get( "utf" ) );
                    assertEquals("ôo", r.get("dIso"));
                    assertEquals("ôo", r.get("dUtf"));
                });
    }


    @Test
    public void testValidateFalse() throws Exception {
        TestUtil.testResult(db, "CALL apoc.util.validate(false,'message',null)", r -> assertEquals(false,r.hasNext()));
    }

    @Test
    public void testValidateTrue() throws Exception {
        try {
            db.execute("CALL apoc.util.validate(true,'message %d',[42])").close();
            fail("should have failed");
        } catch(QueryExecutionException qee) {
            assertEquals("Failed to invoke procedure `apoc.util.validate`: Caused by: java.lang.RuntimeException: message 42",qee.getCause().getCause().getMessage());
        }
    }

    @Test
    public void testSleep() {
        String cypherSleep = "call apoc.util.sleep({duration})";
        testCallEmpty(db, cypherSleep, MapUtil.map("duration", 0l));  // force building query plan

        long duration = 300;
        TestUtil.assertDuration(Matchers.greaterThanOrEqualTo(duration), () -> {
            testCallEmpty(db, cypherSleep, MapUtil.map("duration", duration));
            return null;
        });
    }

    @Test
    public void testSleepWithTerminate() {
        String cypherSleep = "call apoc.util.sleep({duration})";
        testCallEmpty(db, cypherSleep, MapUtil.map("duration", 0l));  // force building query plan

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
