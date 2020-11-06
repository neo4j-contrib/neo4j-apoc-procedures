package apoc.load;

import apoc.util.TestUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.BeforeClass;
import org.junit.Test;
import org.hamcrest.Matchers;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_LOAD_DIRECTORY_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.load.LoadDirectoryItem.DEFAULT_EVENT_KINDS;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class LoadDirectoryTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static GraphDatabaseService db;
    private static File importFolder;

    private static String IMPORT_DIR = "Mätrix II ü 哈哈\uD83D\uDE04123 ; ? : @ & = + $ \\ ";
    private static String SUBFOLDER_1 = "sub folder 哈哈 ü Æ Èì € Œ Ō";
    private static final String SUBFOLDER_2 = "sub folder Two αβγ";
    private static final String SUBFOLDER_3 = "sub folder Three $ Ω";
    private static final String FILE_PROTOCOL = "file://";

    @BeforeClass
    public static void setUp() throws Exception {
        importFolder = new File(temporaryFolder.getRoot() + File.separator + IMPORT_DIR);
        DatabaseManagementService databaseManagementService = new TestDatabaseManagementServiceBuilder(importFolder).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

        TestUtil.registerProcedure(db, LoadDirectory.class, LoadCsv.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);

        // create temp files and subfolder
        temporaryFolder.newFile("Foo.csv");
        temporaryFolder.newFile("Bar.csv");
        temporaryFolder.newFile("Baz.xls");
        temporaryFolder.newFolder(IMPORT_DIR + File.separator + SUBFOLDER_1);
        temporaryFolder.newFolder(IMPORT_DIR + File.separator + SUBFOLDER_2);
        temporaryFolder.newFolder(IMPORT_DIR + File.separator + SUBFOLDER_3);
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestCsv1.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestCsv2.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestCsv3.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestXls1.xls");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "TestJson1.json");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + SUBFOLDER_1 + File.separator + "TestSubfolder.json");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + SUBFOLDER_1 + File.separator + "TestSubfolder.csv");
    }

    @Test
    public void testWithNullUrlDir() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        TestUtil.testFail(db, "CALL apoc.load.directory('*', null, {recursive: false}) YIELD value RETURN value", IllegalArgumentException.class);
    }

    @After
    public void clearDB() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test(expected = RuntimeException.class)
    public void testAddListenerWithApocLoadDirectoryNotEnabled() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, false);
        try {
            db.executeTransactionally("CALL apoc.load.directory.add('test','CREATE (n:Test)', '*.csv', '', {}) YIELD name RETURN name");
        } catch (RuntimeException e) {
            assertEquals("Failed to invoke procedure `apoc.load.directory.add`: Caused by: java.lang.RuntimeException: " + LoadDirectory.NOT_ENABLED_ERROR, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testAddMultipleFoldersAndRemoveAll() throws InterruptedException, IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);

        final String importWithSlashEscaped = IMPORT_DIR.replace("\\", "\\\\");
        File subTempFolder = new File(temporaryFolder.getRoot() + File.separator + importWithSlashEscaped + File.separator + SUBFOLDER_1);
        String subTempFolderUrl = FILE_PROTOCOL + subTempFolder;
        File subTempFolderTwo = new File(temporaryFolder.getRoot() + File.separator + importWithSlashEscaped + File.separator + SUBFOLDER_2);
        String subTempFolderUrlTwo = FILE_PROTOCOL + subTempFolderTwo;
        File subTempFolder3 = new File(temporaryFolder.getRoot() + File.separator + importWithSlashEscaped + File.separator + SUBFOLDER_3);
        String subTempFolderUrl3 = FILE_PROTOCOL + subTempFolder3;

        final Map<String, Object> defaultConfig = Map.of("eventKinds", DEFAULT_EVENT_KINDS, "interval", 1000L);
        testCall(db, "CALL apoc.load.directory.add('testOne','CREATE (n:TestOne)','*.csv','" + subTempFolderUrl + "', {})", result -> {
            assertEquals("testOne", result.get("name"));
            assertEquals("*.csv", result.get("pattern"));
            assertEquals(subTempFolderUrl.replace("\\\\", "\\"), result.get("urlDir"));
            assertEquals("CREATE (n:TestOne)", result.get("cypher"));
            assertEquals(defaultConfig, result.get("config"));
            assertEquals(LoadDirectoryItem.Status.CREATED.name(), result.get("status"));
            assertEquals(StringUtils.EMPTY, result.get("error"));
        });

        db.executeTransactionally("CALL apoc.load.directory.add('testTwo','CREATE (n:TestTwo)','*.json','" + subTempFolderUrlTwo + "', {}) YIELD name RETURN name");

        testResult(db, "CALL apoc.load.directory.add('test3','CREATE (n:Test3)','*.html','" + subTempFolderUrl3 + "', {}) YIELD name RETURN name", result -> {
            List<String> rows = Iterators.asList(result.columnAs("name"));
            assertThat(rows, Matchers.containsInAnyOrder("testOne", "testTwo", "test3"));
        });

        final String queryCount = "MATCH (n:TestOne), (m:TestTwo), (l:Test3) RETURN count(n)+count(m)+count(l) AS count";
        testResult(db, queryCount, result -> assertEquals(0L, result.columnAs("count").next()));

        temporaryFolder.newFile(IMPORT_DIR + File.separator + SUBFOLDER_1 + File.separator + "newFile.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + SUBFOLDER_2 + File.separator + "newFile.json");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + SUBFOLDER_3 + File.separator + "newFile.html");

        assertEventually(() -> db.executeTransactionally(queryCount,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(3L), 30L, TimeUnit.SECONDS);

        testResult(db, "CALL apoc.load.directory.list() YIELD name RETURN name", result -> {
            List<Map<String, Object>> rows = Iterators.asList(result.columnAs("name"));
            assertEquals(3, rows.size());
        });

        testCallEmpty(db, "CALL apoc.load.directory.removeAll", emptyMap());
        testCallEmpty(db, "CALL apoc.load.directory.list", emptyMap());

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newFileTwo.csv");
        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newFileTwo.json");
        Thread.sleep(10000);

        testResult(db, queryCount, result -> assertEquals(3L, result.columnAs("count").next()));

        new File(importFolder + File.separator + SUBFOLDER_1 + File.separator + "newFile.csv").delete();
        new File(importFolder + File.separator + SUBFOLDER_2 + File.separator + "newFile.json").delete();
        new File(importFolder + File.separator + SUBFOLDER_3 + File.separator + "newFile.html").delete();
        new File(importFolder + File.separator + "newFileTwo.csv").delete();
        new File(importFolder + File.separator + "newFileTwo.json").delete();
    }

    @Test
    public void testAddFolderListenerWithAllEventKinds() throws IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        db.executeTransactionally("CALL apoc.load.directory.add('testAllEvents','CREATE (n:TestEntry)','*.csv') YIELD name RETURN name");

        final String queryCount = "MATCH (n:TestEntry) RETURN count(n) AS count";
        testResult(db, queryCount, result -> assertEquals(0L, result.columnAs("count").next()));

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newFile.csv");

        assertEventually(() -> db.executeTransactionally(queryCount,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 30L, TimeUnit.SECONDS);

        new File(importFolder + File.separator + "newFile.csv").delete();

        assertEventually(() -> db.executeTransactionally(queryCount,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(2L), 30L, TimeUnit.SECONDS);

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newFileTwo.csv");

        assertEventually(() -> db.executeTransactionally(queryCount,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(3L), 30L, TimeUnit.SECONDS);

        testCallEmpty(db, "CALL apoc.load.directory.remove('testAllEvents')", emptyMap());

        new File(importFolder + File.separator + "newFile.csv").delete();
        new File(importFolder + File.separator + "newFileTwo.csv").delete();
    }

    @Test
    public void testAddFolderListenerWithSpecificEventKind() throws InterruptedException, IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        db.executeTransactionally("CALL apoc.load.directory.add('testSpecific','CREATE (n:TestEntry {fileName: $fileName})','*.csv', '', {eventKinds: ['ENTRY_CREATE']}) YIELD name RETURN name");

        final String queryCount = "MATCH (n:TestEntry {fileName: $fileName}) RETURN count(n) AS count";
        final String fileOne = "newFile.csv";
        testResult(db, queryCount, Map.of("fileName", fileOne), result -> assertEquals(0L, result.columnAs("count").next()));


        temporaryFolder.newFile(IMPORT_DIR + File.separator + fileOne);

        assertEventually(() -> db.executeTransactionally(queryCount,
                Map.of("fileName", fileOne), (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 30L, TimeUnit.SECONDS);

        new File(importFolder + File.separator + fileOne).delete();
        Thread.sleep(10000);

        testResult(db, queryCount, Map.of("fileName", fileOne), result -> assertEquals(1L, result.columnAs("count").next()));

        final String fileTwo = "newFileTwo.csv";
        temporaryFolder.newFile(IMPORT_DIR + File.separator + fileTwo);

        assertEventually(() -> db.executeTransactionally(queryCount,
                Map.of("fileName", fileTwo), (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 30L, TimeUnit.SECONDS);

        testCallEmpty(db, "CALL apoc.load.directory.remove('testSpecific')", emptyMap());
        new File(importFolder + File.separator + fileTwo).delete();
    }

    @Test
    public void testRemoveListenerWithNotExistingName() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        try {
            testResult(db, "CALL apoc.load.directory.remove('notExisting') YIELD name, pattern, cypher RETURN name, pattern, cypher", result -> {
            });
        } catch (RuntimeException e) {
            String expectedMessage = "Listener with name: notExisting doesn't exists";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testFolderListenerList() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        db.executeTransactionally("CALL apoc.load.directory.add('testOne','CREATE (n:Test)','*.csv','', {}) YIELD name RETURN name");
        db.executeTransactionally("CALL apoc.load.directory.add('testTwo','CREATE (n:Test)', '*.json', '', {}) YIELD name RETURN name");

        final Map<String, Object> defaultConfig = Map.of("eventKinds", DEFAULT_EVENT_KINDS, "interval", 1000L);
        testResult(db, "CALL apoc.load.directory.list()", result -> {
            Map<String, Object> mapTestOne = result.next();
            assertThat(mapTestOne.get("name"), isOneOf("testOne", "testTwo"));
            assertThat(mapTestOne.get("pattern"), isOneOf("*.csv", "*.json"));
            assertEquals("", mapTestOne.get("urlDir"));
            assertEquals("CREATE (n:Test)", mapTestOne.get("cypher"));
            assertEquals(defaultConfig, mapTestOne.get("config"));
            assertEquals(LoadDirectoryItem.Status.RUNNING.name(), mapTestOne.get("status"));
            assertEquals(StringUtils.EMPTY, mapTestOne.get("error"));
            Map<String, Object> mapTestTwo = result.next();
            assertThat(mapTestTwo.get("name"), isOneOf("testOne", "testTwo"));
            assertThat(mapTestTwo.get("pattern"), isOneOf("*.csv", "*.json"));
            assertEquals("", mapTestTwo.get("urlDir"));
            assertEquals("CREATE (n:Test)", mapTestTwo.get("cypher"));
            assertEquals(defaultConfig, mapTestTwo.get("config"));
            assertEquals(LoadDirectoryItem.Status.RUNNING.name(), mapTestTwo.get("status"));
            assertEquals(StringUtils.EMPTY, mapTestTwo.get("error"));
            assertFalse(result.hasNext());
        });

        testCall(db, "CALL apoc.load.directory.remove('testOne')", result -> {
            // remains 2nd listener
            assertEquals("testTwo", result.get("name"));
            assertEquals("*.json", result.get("pattern"));
            assertEquals("", result.get("urlDir"));
            assertEquals("CREATE (n:Test)", result.get("cypher"));
            assertEquals(defaultConfig, result.get("config"));
            assertEquals(LoadDirectoryItem.Status.RUNNING.name(), result.get("status"));
            assertEquals(StringUtils.EMPTY, result.get("error"));
        });

        testCallEmpty(db, "CALL apoc.load.directory.remove('testTwo') YIELD name RETURN name", emptyMap());
    }

    @Test
    public void testRemoveFolderListener() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        db.executeTransactionally("CALL apoc.load.directory.add('test','CREATE (n:Test)','*.csv', '', {}) YIELD name RETURN name");
        testResult(db, "CALL apoc.load.directory.list() YIELD name RETURN name", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("name"));
                    assertEquals(1, rows.size());
                }
        );

        testCallEmpty(db, "CALL apoc.load.directory.remove('test')", emptyMap());
        testCallEmpty(db, "CALL apoc.load.directory.list", emptyMap());
    }

    @Test
    public void testFolderListenerNotMatchingPattern() throws IOException, InterruptedException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        db.executeTransactionally("CALL apoc.load.directory.add('test','CREATE (n:Test {file: $fileName})','*.json','', {}) YIELD name RETURN name");
        Thread.sleep(2000);

        final String queryCount = "MATCH (n:Test {file: $file}) RETURN count(n) AS count";
        testResult(db, queryCount, Map.of("file", "newFile.csv"), result -> assertEquals(0L, result.columnAs("count").next()));

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newFile.csv");

        Thread.sleep(10000);
        testResult(db, queryCount, Map.of("file", "newFile.csv"), result -> assertEquals(0L, result.columnAs("count").next()));

        testCallEmpty(db, "CALL apoc.load.directory.remove('test')", emptyMap());

        File file = new File(importFolder + File.separator + "newFile.csv");
        file.delete();
    }

    @Test
    public void testFolderListenerWithSameName() throws IOException, InterruptedException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        testCall(db, "CALL apoc.load.directory.add('sameName','CREATE (n:Test {event: $eventKind, url: $fileDirectory})','*csv','', {interval: 30000}) YIELD name RETURN name",
                r -> assertEquals("sameName", r.get("name"))
        );

        testCall(db, "CALL apoc.load.directory.add('sameName','CREATE (n:TestTwo {event: $eventKind, url: $fileDirectory, file: $fileName })','*csv','', {}) YIELD name RETURN name",
                r -> assertEquals("sameName", r.get("name"))
        );
        Thread.sleep(2000);

        final String queryCount = "MATCH (n:TestTwo {event: $event, url: $url, file: $file }) RETURN count(n) AS count";
        final Map<String, Object> queryParams = Map.of("file", "newFile.csv", "url", temporaryFolder.getRoot().getPath() + File.separator + IMPORT_DIR, "event", "ENTRY_CREATE");

        testResult(db, queryCount, queryParams, result -> assertEquals(0L, result.columnAs("count").next()));

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newFile.csv");

        assertEventually(() -> db.executeTransactionally(queryCount, queryParams, (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 20L, TimeUnit.SECONDS);

        testResult(db, "MATCH (n:Test) RETURN count(n) AS count", result -> assertEquals(0L, result.columnAs("count").next()));

        testCallEmpty(db, "CALL apoc.load.directory.remove('sameName')", emptyMap());

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newFileTwo.csv");
        Thread.sleep(10000);

        testResult(db, "MATCH (n:TestTwo) RETURN count(n) AS count", result -> assertEquals(1L, result.columnAs("count").next()));
        testResult(db, "MATCH (n:Test) RETURN count(n) AS count", result -> assertEquals(0L, result.columnAs("count").next()));

        new File(importFolder + File.separator + "newFile.csv").delete();
        new File(importFolder + File.separator + "newFileTwo.csv").delete();
    }

    @Test
    public void testAddFolderWithOnlyNameAndCypher() throws InterruptedException, IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        testResult(db, "CALL apoc.load.directory.add('testOnlyNameAndCypher','CREATE (n:TestOnly {prop: $filePath})')",
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals("testOnlyNameAndCypher", row.get("name"));
                    assertFalse(result.hasNext());
                });
        Thread.sleep(2000);

        final String relativePath = IMPORT_DIR + File.separator + "newFile.exe";
        final String absolutePath = temporaryFolder.getRoot().getPath() + File.separator + relativePath;
        final String queryCount = "MATCH (n:TestOnly {prop: $path}) RETURN count(n) AS count";

        testResult(db, queryCount, Map.of("path", absolutePath), result -> assertEquals(0L, result.columnAs("count").next()));

        temporaryFolder.newFile(relativePath);
        assertEventually(() -> db.executeTransactionally(queryCount,
                Map.of("path", absolutePath), (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 20L, TimeUnit.SECONDS);

        testCallEmpty(db, "CALL apoc.load.directory.removeAll", emptyMap());

        new File(importFolder + File.separator + "newFile.exe").delete();
    }

    @Test
    public void testAddFolderWithRelativePath() throws InterruptedException, IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        testCall(db, "CALL apoc.load.directory.add('testRelativePath','CREATE (n:TestRelativePath)', '" + SUBFOLDER_1 + "')",
                r -> assertEquals("testRelativePath", r.get("name"))
        );

        final String queryCount = "MATCH (n:TestRelativePath) RETURN count(n) AS count";
        testResult(db, queryCount, result -> assertEquals(0L, result.columnAs("count").next()));


        temporaryFolder.newFile(IMPORT_DIR + File.separator + SUBFOLDER_1 + File.separator + "newFile.kt");

        assertEventually(() -> db.executeTransactionally(queryCount,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 20L, TimeUnit.SECONDS);

        testCallEmpty(db, "CALL apoc.load.directory.removeAll", emptyMap());

        new File(importFolder + File.separator + SUBFOLDER_1 + File.separator + "newFile.kt").delete();
    }

    @Test
    public void testAddFolderMultipleListener() throws InterruptedException, IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);

        db.executeTransactionally("CALL apoc.load.directory.add('testOne','CREATE (n:TestOne)', '*.csv', '', {}) YIELD name RETURN name");
        db.executeTransactionally("CALL apoc.load.directory.add('testTwo','CREATE (n:TestTwo)', '*.json', '', {}) YIELD name RETURN name");

        final String queryCountLabelOne = "MATCH (n:TestOne) RETURN count(n) AS count";
        final String queryCountLabelTwo = "MATCH (n:TestTwo) RETURN count(n) AS count";
        testResult(db, queryCountLabelOne, result -> assertEquals(0L, result.columnAs("count").next()));
        testResult(db, queryCountLabelTwo, result -> assertEquals(0L, result.columnAs("count").next()));

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newOtherFile.csv");

        assertEventually(() -> db.executeTransactionally(queryCountLabelOne,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 30L, TimeUnit.SECONDS);


        testCall(db, "CALL apoc.load.directory.remove('testOne')", result -> {
            // remain 1st listener
            assertEquals("testTwo", result.get("name"));
            assertEquals("*.json", result.get("pattern"));
            assertEquals("", result.get("urlDir"));
            assertEquals("CREATE (n:TestTwo)", result.get("cypher"));
            assertEquals("CREATE (n:TestTwo)", result.get("cypher"));
        });

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newOtherFile.json");

        assertEventually(() -> db.executeTransactionally(queryCountLabelTwo,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 30L, TimeUnit.SECONDS);

        testResult(db, queryCountLabelOne, result -> assertEquals(1L, result.columnAs("count").next()));

        testCallEmpty(db, "CALL apoc.load.directory.remove('testTwo')", emptyMap());

        new File(importFolder + File.separator + "newFile.csv").delete();
        new File(importFolder + File.separator + "newOtherFile.csv").delete();
        new File(importFolder + File.separator + "newOtherFile.json").delete();
    }

    @Test
    public void testAddFolderListener() throws InterruptedException, IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        testCall(db, "CALL apoc.load.directory.add('test','CREATE (n:Test)','*.csv','', {}) YIELD name RETURN name",
                r -> assertEquals("test", r.get("name"))
        );

        final String queryCount = "MATCH (n:Test) RETURN count(n) AS count";
        testResult(db, queryCount, result -> assertEquals(0L, result.columnAs("count").next()));

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newFile.csv");

        assertEventually(() -> db.executeTransactionally(queryCount,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 30L, TimeUnit.SECONDS);


        try (Transaction tx = db.beginTx()) {
            assertEquals(1, Iterators.count(tx.findNodes(Label.label("Test"))));
            tx.commit();
        }

        new File(importFolder + File.separator + "newFile.csv").delete();

        assertEventually(() -> db.executeTransactionally(queryCount,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(2L), 30L, TimeUnit.SECONDS);

        testCallEmpty(db, "CALL apoc.load.directory.remove('test')", emptyMap());

        temporaryFolder.newFile(IMPORT_DIR + File.separator + "newOtherFile.csv");

        Thread.sleep(10000);
        testResult(db, queryCount, result -> assertEquals(2L, result.columnAs("count").next()));

        new File(importFolder + File.separator + "newOtherFile.csv").delete();
    }

    @Test
    public void testAddLoadFolderWithFileProtocol() throws InterruptedException, IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        apocConfig().setProperty(APOC_LOAD_DIRECTORY_ENABLED, true);
        File rootTempFolder = temporaryFolder.getRoot();
        String folderAsExternalUrl = FILE_PROTOCOL + rootTempFolder;

        testResult(db, "CALL apoc.load.directory.add('testExternalUrl','CREATE (n:TestExternalUrl {prop: $fileName})','*.csv','" + folderAsExternalUrl + "', {}) YIELD name RETURN name",
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals("testExternalUrl", row.get("name"));
                    assertFalse(result.hasNext());
                });
        Thread.sleep(2000);
        final String queryCount = "MATCH (n:TestExternalUrl {prop: 'newFile.csv'}) RETURN count(n) AS count";
        testResult(db, queryCount, result ->assertEquals(0L, result.columnAs("count").next()));

        temporaryFolder.newFile("newFile.csv");

        assertEventually(() -> db.executeTransactionally(queryCount,
                emptyMap(), (r) -> r.columnAs("count").next()),
                value -> value.equals(1L), 20L, TimeUnit.SECONDS);

        try (Transaction tx = db.beginTx()) {
            assertEquals(1, Iterators.count(tx.findNodes(Label.label("TestExternalUrl"))));
            tx.commit();
        }

        new File(temporaryFolder.getRoot() + File.separator + "newFile.csv").delete();

        testCallEmpty(db, "CALL apoc.load.directory.removeAll", emptyMap());
    }

    @Test
    public void testWithSubfolder() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        testResult(db, "CALL apoc.load.directory('*', '" + SUBFOLDER_1 + "', {recursive: false}) YIELD value RETURN value", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("value"));
                    assertTrue(rows.contains(SUBFOLDER_1 + File.separator + "TestSubfolder.json"));
                    assertTrue(rows.contains(SUBFOLDER_1 + File.separator + "TestSubfolder.csv"));
                    assertEquals(2, rows.size());
                }
        );
    }

    @Test
    public void testWithFileProtocolAndRecursiveFalse() {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        File rootTempFolder = temporaryFolder.getRoot();
        String folderAsExternalUrl = FILE_PROTOCOL + rootTempFolder;
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
                    assertTrue(rows.contains(SUBFOLDER_1 + File.separator + "TestSubfolder.json"));
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
                    assertTrue(rows.contains(SUBFOLDER_1 + File.separator + "TestSubfolder.csv"));
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