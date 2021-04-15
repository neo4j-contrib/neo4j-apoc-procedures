package apoc.util;

import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCallEmpty;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 26.05.16
 */
public class UtilsTest {

    private static final String SIMPLE_STRING = "Test";
    private static final String COMPLEX_STRING = "Mätrix II 哈哈\uD83D\uDE04123";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Utils.class);
    }

    @Test
    public void testMultipleCharsetsCompressionWithDifferentResults() throws Exception {

        List<String> listCompressed = new ArrayList<>();

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-8'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value")))
        );

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-16'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value")))
        );

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-16BE'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value")))
        );

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-16LE'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value")))
        );

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {charset: 'ISO-8859-1'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value")))
        );

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-32'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value")))
        );

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {charset: 'US-ASCII'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value")))
        );

        // expected all different compressed string in complex string
        long sizeArray = listCompressed.size();
        assertEquals(sizeArray, new HashSet<>(listCompressed).size());
    }

    @Test
    public void testValueMainCompressorAlgoOnSimpleString() throws Exception {

        String TEST_TO_GZIP = "H4sIAAAAAAAA/wtJLS4BADLRTXgEAAAA";
        String TEST_TO_DEFLATE = "eJwLSS0uAQAD3QGh";
        String TEST_TO_BZIP2 = "QlpoOTFBWSZTWdliiV0AAAADgAQAAgAMACAAMM00GaaJni7kinChIbLFEro=";

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {compression: 'GZIP'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(TEST_TO_GZIP, encodeBase64FromBytesToString((byte[]) r.get("value")))
        );

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {compression: 'BZIP2'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(TEST_TO_BZIP2, encodeBase64FromBytesToString((byte[]) r.get("value")))
        );

        TestUtil.testCall(db,
                "RETURN apoc.util.compress($text, {compression: 'DEFLATE'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(TEST_TO_DEFLATE, encodeBase64FromBytesToString((byte[]) r.get("value")))
        );
    }

    @Test(expected = RuntimeException.class)
    public void testWrongDecompressionFromPreviousDifferentCompressionAlgo() throws Exception {
        try {
            TestUtil.testCall(db, "WITH apoc.util.compress('test', {compression: 'GZIP'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'DEFLATE'}) AS value", r -> {
            });
        } catch (RuntimeException e) {
            String expectedMessage = "Failed to invoke function `apoc.util.decompress`: Caused by: java.util.zip.ZipException: incorrect header check";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testWrongDecompressionFromPreviousDifferentCharset() throws Exception {

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'UTF-8'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-8'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'UTF-16'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'UTF-8'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertNotEquals(COMPLEX_STRING, r.get("value"))
        );
    }

    @Test
    public void testCompressAndDecompressWithMultipleCompressionCharsetsReturningStartString() throws Exception {

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text) AS compressed RETURN apoc.util.decompress(compressed) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'UTF-8'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-8'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'UTF-16'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'UTF-16BE'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16BE'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'UTF-16LE'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16LE'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'UTF-32'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-32'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'US-ASCII'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'US-ASCII'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {charset: 'ISO-8859-1'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'ISO-8859-1'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value"))
        );
    }

    @Test
    public void testCompressAndDecompressWithMultipleCompressionAlgosReturningStartString() throws Exception {

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text) AS compressed RETURN apoc.util.decompress(compressed) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {compression: 'BZIP2'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'BZIP2'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {compression: 'DEFLATE'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'DEFLATE'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {compression: 'BLOCK_LZ4'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'BLOCK_LZ4'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value"))
        );

        TestUtil.testCall(db,
                "WITH apoc.util.compress($text, {compression: 'FRAMED_SNAPPY'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'FRAMED_SNAPPY'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value"))
        );
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
    public void testValidateFalse() throws Exception {
        TestUtil.testResult(db, "CALL apoc.util.validate(false,'message',null)", r -> assertEquals(false,r.hasNext()));
    }

    @Test
    public void testValidateTrue() throws Exception {
        try {
            db.executeTransactionally("CALL apoc.util.validate(true,'message %d',[42])");
            fail("should have failed");
        } catch(QueryExecutionException qee) {
            assertEquals("Failed to invoke procedure `apoc.util.validate`: Caused by: java.lang.RuntimeException: message 42",qee.getCause().getCause().getMessage());
        }
    }

    @Test
    public void testValidatePredicateReturn() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.util.validatePredicate(false,'message',null) AS value", r -> assertEquals(true, r.get("value")));
    }

    @Test
    public void testValidatePredicateTrue() throws Exception {
        db.executeTransactionally("CREATE (:Person {predicate: true})");
        TestUtil.testFail(db, "MATCH (n:Person) RETURN apoc.util.validatePredicate(n.predicate,'message %d',[42]) AS n", QueryExecutionException.class);
    }

    @Test
    public void testSleep() {
        String cypherSleep = "call apoc.util.sleep($duration)";
        testCallEmpty(db, cypherSleep, MapUtil.map("duration", 0l));  // force building query plan

        long duration = 300;
        TestUtil.assertDuration(Matchers.greaterThanOrEqualTo(duration), () -> {
            testCallEmpty(db, cypherSleep, MapUtil.map("duration", duration));
            return null;
        });
    }

    @Test
    public void testSleepWithTerminate() {
        String cypherSleep = "call apoc.util.sleep($duration)";
        testCallEmpty(db, cypherSleep, MapUtil.map("duration", 0l));  // force building query plan

        long duration = 300;
        TestUtil.assertDuration(Matchers.lessThan(duration), () -> {
            final Transaction[] tx = new Transaction[1];

            Future future = Executors.newSingleThreadScheduledExecutor().submit( () -> {
                tx[0] = db.beginTx();
                try {
                    Result result = tx[0].execute(cypherSleep, MapUtil.map("duration", 10000));
                    tx[0].commit();
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

    private String encodeBase64FromBytesToString(byte[] list) {
        return Base64.getEncoder().encodeToString(list);
    }

}
