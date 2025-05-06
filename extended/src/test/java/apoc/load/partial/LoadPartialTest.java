package apoc.load.partial;

import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import apoc.util.Utils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.*;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LoadPartialTest {

    public static final String PARTIAL_CSV = "Rana,11\nSelina,";
    public static final String PARTIAL_CSV_WITHOUT_LIMIT = PARTIAL_CSV + "18\n";
    private static final String COMPLEX_STRING = "Mätrix II 哈哈\uD83D\uDE04123";
    private static final String COMPLEX_STRING_PARTIAL = COMPLEX_STRING.substring(4, 15);
    public static final String EXPECTED_PARTIAL_JSON_ARCHIVE = """
            ,
             "age": 41,
            \s""";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadPartial.class, LoadCsv.class, Utils.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
    }
    
    @Test
    public void testLoadPartialWithImportNotEnabled() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, false);

        URL urlFileName = getUrlFileName("test.csv");
        String path = urlFileName.getPath();
        
        assertFails(db, "CALL apoc.load.stringPartial($url, 17, 15)", Map.of("url", path),
                LOAD_FROM_FILE_ERROR);

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
    }
    
    @Test
    public void testLoadPartialString() {
        URL urlFileName = getUrlFileName("test.csv");
        String path = urlFileName.getPath();
        testPartialCsvCommon(path);
    }
    
    @Test
    public void testLoadPartialStringWithoutLimit() {
        URL urlFileName = getUrlFileName("test.csv");
        String path = urlFileName.getPath();
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17)",
                map("url", path));

        assertEquals(PARTIAL_CSV_WITHOUT_LIMIT, output);
    }

    @Test
    public void testLoadPartialStringByUrl() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/neo4j-contrib/neo4j-apoc-procedures/refs/heads/dev/extended/src/test/resources/test.csv");
        String path = url.toString();
        testPartialCsvCommon(path);
    }

    @Test
    public void testLoadPartialStringByUrlWithoutLimit() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/neo4j-contrib/neo4j-apoc-procedures/refs/heads/dev/extended/src/test/resources/test.csv");
        String path = url.toString();
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17)",
                map("url", path));
        
        assertEquals(PARTIAL_CSV_WITHOUT_LIMIT, output);
    }
    
    @Test
    public void testCompareWithLoadCsvLargeFile() throws Exception {

        // 30MB file
        URL urlFileName = new URL("https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/Balance-of-payments-and-international-investment-position-September-2024-quarter/Download-data/balance-of-payments-and-international-investment-position-september-2024-quarter.csv");

        String path = urlFileName.toString();
        
        // about the same portion of code (i.e. after ~150 thousand lines)
        long startPartial = System.currentTimeMillis();
        int limit = 19000;
        String outputPartial = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, $offset, $limit)",
                map("url", path, "offset", 180 * 150_000, "limit", limit));
        
        long timePartial = System.currentTimeMillis() - startPartial;
        assertEquals(limit, outputPartial.length());
        
        
        long startLoadCsv = System.currentTimeMillis();
        List<List<String>> resCsv = db.executeTransactionally("CALL apoc.load.csv($url, {results:['strings']}) YIELD strings RETURN strings SKIP 150000 LIMIT 100",
                map("url", path),
                r -> r.<List<String>>columnAs("strings").stream().toList()
        );

        long timeLoadCsv = System.currentTimeMillis() - startLoadCsv;
        
        // we make the joining later, since it requires additional time that is beyond the scope of the procedure
        String outputLoadCsv = resCsv.stream()
                .map(i -> String.join("", i))
                .collect(Collectors.joining());

        // difficult to get the exact portion, so we just check that the output of load.partial is greater
        String messageLength = String.format("Current lengths are %s and %s: ", 
                outputPartial.length(), outputLoadCsv.length());
        assertTrue(messageLength, 
                outputPartial.length() > outputLoadCsv.length());

        // nevertheless the portion is greater, the time is less than half
        String messageTime = String.format("Current times are %s and %s: ", 
                timePartial, timeLoadCsv);
        assertTrue(messageTime, 
                timeLoadCsv > timePartial * 2);

    }

    private void testPartialCsvCommon(String path) {
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", path));
        
        assertEquals(PARTIAL_CSV, output);
    }
    
    @Test
    public void testLoadPartialStringTarGzByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j/apoc/blob/dev/core/src/test/resources/testload.tar.gz?raw=true");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)", 
                map("url",url.toString()+"!csv/test.csv"));

        assertEquals(PARTIAL_CSV, output);
    }
    
    @Test
    public void testLoadPartialStringTarGz() {
        URL url = getUrlFileName("testload.tar.gz");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(EXPECTED_PARTIAL_JSON_ARCHIVE, output);
    }
    
    @Test
    public void testLoadPartialStringTarGzWithoutLimit() {
        URL url = getUrlFileName("testload.tar.gz");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(",\n" +
                " \"age\": 41,\n" +
                " \"children\": [\"Selina\",\"Rana\",\"Selma\"]\n" +
                "}\n", output);
    }
    
    @Test
    public void testLoadPartialStringTgz() {
        URL url = getUrlFileName("testload.tgz");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(EXPECTED_PARTIAL_JSON_ARCHIVE, output);
    }
    
    
    @Test
    public void testLoadPartialStringTar() {
        URL url = getUrlFileName("testload.tar");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(EXPECTED_PARTIAL_JSON_ARCHIVE, output);
    }

    @Test
    public void testLoadPartialStringZip() {
        URL url = getUrlFileName("testload.zip");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(EXPECTED_PARTIAL_JSON_ARCHIVE, output);
    }
    
    @Test
    public void testLoadPartialStringWithBinaryAndMultipleCompressionAlgo() {

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'GZIP'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'GZIP'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'BZIP2'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'BZIP2'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'DEFLATE'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'DEFLATE'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'BLOCK_LZ4'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'BLOCK_LZ4'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'FRAMED_SNAPPY'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'FRAMED_SNAPPY'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'NONE'}) AS compressed " +
                        "CALL apoc.load.stringPartial(compressed, 5, 17, {compression: 'NONE'}) YIELD value RETURN value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING_PARTIAL, r.get("value")));
    }
}
