package apoc.load;

import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.*;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;


public class LoadDirectoryTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static GraphDatabaseService db;

    private static String IMPORT_DIR = "Mätrix II ü 哈哈\uD83D\uDE04123 ; ? : @ & = + $ \\ ";
    private static String SUBFOLDER = "sub folder 哈哈 ü Æ Èì € Œ Ō";

    @BeforeClass
    public static void setUp() throws Exception {
        File importFolder = new File(temporaryFolder.getRoot() + File.separator + IMPORT_DIR);
        DatabaseManagementService databaseManagementService = new TestDatabaseManagementServiceBuilder(importFolder).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

        TestUtil.registerProcedure(db, LoadDirectory.class);

        // create temp files and subfolder
        temporaryFolder.newFile("Foo.csv");
        temporaryFolder.newFile("Bar.csv");
        temporaryFolder.newFile("Baz.xsl");
        temporaryFolder.newFolder(IMPORT_DIR + File.separator + SUBFOLDER);
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestCsv1.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestCsv2.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestCsv3.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "Testxsl1.xsl");
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
                    assertTrue(rows.contains(rootTempFolder + File.separator + "Baz.xsl"));
                    assertEquals(3, rows.size());
                }
        );
    }

    @Test
    public void testWithFileProtocolFilterXslAndRecursiveTrue() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        File rootTempFolder = temporaryFolder.getRoot();
        String folderAsExternalUrl = "file://" + rootTempFolder;
        testResult(db, "CALL apoc.load.directory('*.xsl', '" + folderAsExternalUrl + "', {recursive: true}) YIELD value RETURN value", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("value"));
                    assertTrue(rows.contains(rootTempFolder + File.separator + "Baz.xsl"));
                    assertTrue(rows.contains("Testxsl1.xsl"));
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
                    assertTrue(rows.contains("Testxsl1.xsl"));
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
}
