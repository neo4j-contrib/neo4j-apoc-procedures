package apoc.load.partial;

import apoc.util.TestUtil;
import apoc.util.Utils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.Map;

import static apoc.ApocConfig.*;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LoadPartialTest {

    public static final String PARTIAL_CSV = "Rana,11\nSelina,";
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
        TestUtil.registerProcedure(db, LoadPartial.class, Utils.class);
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
    public void testLoadCsv() {
        URL urlFileName = getUrlFileName("test.csv");
        String path = urlFileName.getPath();
        testPartialCsvCommon(path);
    }
    
    @Test
    public void testLoadCsvWithoutLimit() {
        URL urlFileName = getUrlFileName("test.csv");
        String path = urlFileName.getPath();
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17)",
                map("url", path));

        String expected = "Rana,11\n" +
                "Selina,18\n";
        assertEquals(expected, output);
    }
    
    @Test
    public void testLoadJson() {
        URL urlFileName = getUrlFileName("multi.json");
        String path = urlFileName.getPath();
        // String url = "test.csv";
        testCall(db, "CALL apoc.load.jsonPartial($url, 17)",
                map("url", path),
                r -> {
                    Map<String, Object> expected = map("bar", asList(4L, 5L, 6L));
                    assertEquals(expected, r.get("value"));
                }
        );
    }

    @Test
    public void testLoadCsvByUrl() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/neo4j-contrib/neo4j-apoc-procedures/refs/heads/dev/extended/src/test/resources/test.csv");
        String path = url.toString();
        testPartialCsvCommon(path);
    }

    @Test
    public void testLoadCsvByUrlWithoutLimit() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/neo4j-contrib/neo4j-apoc-procedures/refs/heads/dev/extended/src/test/resources/test.csv");
        String path = url.toString();
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17)",
                map("url", path));

        String expected = "Rana,11\n" +
                "Selina,18\n";
        assertEquals(expected, output);
    }
    
    @Test
    public void testLoadCsvLargeFile() throws Exception {
        // 30MB file
        URL urlFileName = new URL("https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/Balance-of-payments-and-international-investment-position-September-2024-quarter/Download-data/balance-of-payments-and-international-investment-position-september-2024-quarter.csv");
        String path = urlFileName.toString();
        int limit = 300;
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 50, $limit)",
                map("url", path, "limit", limit));

        assertEquals(limit, output.length());
    }

    @Test
    public void testLoadCsvLargeFileZip() throws Exception {
        // 100MB zip file
        // 800MB csv file inside it
        URL urlFileName = new URL("https://www3.stats.govt.nz/2018census/Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB.zip!Data8277.csv");
        String path = urlFileName.toString();
        int limit = 300;
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 50, $limit)",
                map("url", path, "limit", limit));

        assertEquals(limit, output.length());
    }

    private void testPartialCsvCommon(String path) {
        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", path));
        
        assertEquals(PARTIAL_CSV, output);
    }
    
    @Test
    public void testLoadCsvTarGzByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j/apoc/blob/dev/core/src/test/resources/testload.tar.gz?raw=true");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)", 
                map("url",url.toString()+"!csv/test.csv"));

        assertEquals("Rana,11\n" +
                "Selina,", output);
    }
    
    @Test
    public void testLoadJsonTarGz() {
        URL url = getUrlFileName("testload.tar.gz");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(EXPECTED_PARTIAL_JSON_ARCHIVE, output);
    }
    
    @Test
    public void testLoadJsonTarGzWithoutLimit() {
        URL url = getUrlFileName("testload.tar.gz");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(",\n" +
                " \"age\": 41,\n" +
                " \"children\": [\"Selina\",\"Rana\",\"Selma\"]\n" +
                "}\n", output);
    }
    
    @Test
    public void testLoadJsonTgz() {
        URL url = getUrlFileName("testload.tgz");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(EXPECTED_PARTIAL_JSON_ARCHIVE, output);
    }
    
    
    @Test
    public void testLoadJsonTar() {
        URL url = getUrlFileName("testload.tar");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(EXPECTED_PARTIAL_JSON_ARCHIVE, output);
    }

    @Test
    public void testLoadJsonZip() {
        URL url = getUrlFileName("testload.zip");

        String output = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", url.getPath() + "!person.json"));

        assertEquals(EXPECTED_PARTIAL_JSON_ARCHIVE, output);
    }
    
    @Test
    public void testCompressAndDecompressWithMultipleCompressionAlgosReturningStartString() {

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
