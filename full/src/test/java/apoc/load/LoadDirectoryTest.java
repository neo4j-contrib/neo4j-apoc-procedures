package apoc.load;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class LoadDirectoryTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static GraphDatabaseService db;

    private static String IMPORT_DIR = "Mätrix II ü 哈哈\uD83D\uDE04123 ; ? : @ & = + $ \\ ";
    private static String SUBFOLDER = "sub folder 哈哈 ü Æ Èì € Œ Ō";

    @BeforeClass
    public static void setUp() throws Exception {
        File importFolder = new File(temporaryFolder.getRoot() + File.separator + IMPORT_DIR);
        DatabaseManagementService databaseManagementService = new TestDatabaseManagementServiceBuilder(importFolder.toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

        TestUtil.registerProcedure(db, LoadDirectory.class, LoadCsv.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);

        // create temp files and subfolder
        temporaryFolder.newFile("Foo.csv");
        temporaryFolder.newFile("Bar.csv");
        temporaryFolder.newFile("Baz.xls");
        temporaryFolder.newFolder(IMPORT_DIR + File.separator + SUBFOLDER);
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestCsv1.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestCsv2.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestCsv3.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestXls1.xls");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestJson1.json");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + SUBFOLDER + File.separator + "TestSubfolder.json");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + SUBFOLDER + File.separator + "TestSubfolder.csv");
    }

    @Test
    public void testWithNullUrlDir() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        TestUtil.testFail(db, "CALL apoc.load.directory('*', null, {recursive: false}) YIELD value RETURN value", IllegalArgumentException.class);
    }

    @Test
    public void testWithSubfolder() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        testResult(db, "CALL apoc.load.directory('*', '" + SUBFOLDER +"', {recursive: false}) YIELD value RETURN value", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("value"));
                    assertTrue(rows.contains(SUBFOLDER + File.separator + "TestSubfolder.json"));
                    assertTrue(rows.contains(SUBFOLDER + File.separator + "TestSubfolder.csv"));
                    assertEquals(2, rows.size());
                }
        );
    }

    @Test
    public void testWithFileProtocolAndRecursiveFalse() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        File rootTempFolder = temporaryFolder.getRoot();
        String folderAsExternalUrl = "file://" + rootTempFolder;
        testResult(db, "CALL apoc.load.directory('*', '" + folderAsExternalUrl + "', {recursive: false}) YIELD value RETURN value", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("value"));
                    assertTrue(rows.contains(rootTempFolder + File.separator + "Foo.csv"));
                    assertTrue(rows.contains(rootTempFolder + File.separator + "Bar.csv"));
                    assertTrue(rows.contains(rootTempFolder + File.separator + "Baz.xls"));
                    assertEquals(3, rows.size());
                }
        );
    }

    @Test
    public void testWithFileProtocolFilterXlsAndRecursiveTrue() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        File rootTempFolder = temporaryFolder.getRoot();
        String folderAsExternalUrl = "file://" + rootTempFolder;
        testResult(db, "CALL apoc.load.directory('*.xls', '" + folderAsExternalUrl + "', {recursive: true}) YIELD value RETURN value", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("value"));
                    assertTrue(rows.contains(rootTempFolder + File.separator + "Baz.xls"));
                    assertTrue(rows.contains(rootTempFolder + File.separator + IMPORT_DIR + File.separator + "TestXls1.xls"));
                    assertEquals(2, rows.size());
                }
        );
    }

    @Test
    public void testWithFilterAllRecursiveFalse() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        testResult(db, "CALL apoc.load.directory('*', '', {recursive: false}) YIELD value RETURN value", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("value"));
                    assertTrue(rows.contains("TestCsv1.csv"));
                    assertTrue(rows.contains("TestCsv2.csv"));
                    assertTrue(rows.contains("TestCsv3.csv"));
                    assertTrue(rows.contains("TestJson1.json"));
                    assertTrue(rows.contains("TestXls1.xls"));
                    assertEquals(5, rows.size());
                }
        );
    }

    @Test
    public void testWithFilterJsonRecursiveTrue() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        testResult(db, "CALL apoc.load.directory('*.json', '', {recursive: true}) YIELD value RETURN value", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("value"));
                    assertTrue(rows.contains("TestJson1.json"));
                    assertTrue(rows.contains(SUBFOLDER + File.separator + "TestSubfolder.json"));
                    assertEquals(2, rows.size());
                }
        );
    }

    @Test
    public void testWithFilterCsv() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        testResult(db, "CALL apoc.load.directory('*.csv') YIELD value RETURN value", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("value"));
                    assertTrue(rows.contains("TestCsv1.csv"));
                    assertTrue(rows.contains("TestCsv2.csv"));
                    assertTrue(rows.contains("TestCsv3.csv"));
                    assertTrue(rows.contains(SUBFOLDER + File.separator + "TestSubfolder.csv"));
                    assertEquals(4, rows.size());
                }
        );
    }

    @Test
    public void testLoadDirectoryConcatenatedWithLoadCsv() throws URISyntaxException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        File rootTempFolder = Paths.get(getUrlFileName("test.csv").toURI()).getParent().toFile();
        String folderAsExternalUrl = "file://" + rootTempFolder;
        testResult(db, "CALL apoc.load.directory('*.csv', '" + folderAsExternalUrl + "') YIELD value " +
                "WITH value as url WHERE url ENDS WITH 'test.csv' OR url ENDS WITH 'test-pipe-column.csv' WITH url ORDER BY url DESC CALL apoc.load.csv(url, {results:['map']}) YIELD map RETURN map", result -> {
                    Map<String, Object> firstRowFirstFile = (Map<String, Object>) result.next().get("map");
                    assertEquals(Map.of("name", "Selma", "age", "8"), firstRowFirstFile);
                    Map<String, Object> secondRowFirstFile = (Map<String, Object>) result.next().get("map");
                    assertEquals(Map.of("name", "Rana", "age", "11"), secondRowFirstFile);
                    Map<String, Object> thirdRowFirstFile = (Map<String, Object>) result.next().get("map");
                    assertEquals(Map.of("name", "Selina", "age", "18"), thirdRowFirstFile);
                    Map<String, Object> firstRowSecondFile = (Map<String, Object>) result.next().get("map");
                    assertEquals(Map.of("name", "Selma", "beverage", "Soda"), firstRowSecondFile);
                    Map<String, Object> secondRowSecondFile = (Map<String, Object>) result.next().get("map");
                    assertEquals(Map.of("name", "Rana", "beverage", "Tea|Milk"), secondRowSecondFile);
                    Map<String, Object> thirdRowSecondFile = (Map<String, Object>) result.next().get("map");
                    assertEquals(Map.of("name", "Selina", "beverage", "Cola"), thirdRowSecondFile);
                    assertFalse(result.hasNext());
                }
        );
    }
}
