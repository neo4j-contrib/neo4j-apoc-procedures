package apoc.load;

import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;

public class LoadFolderTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void setUp() throws Exception {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(temporaryFolder.getRoot()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

        TestUtil.registerProcedure(db, LoadFolder.class);

        // create temp files and subfolder
        temporaryFolder.newFolder("subfolder");
        temporaryFolder.newFile("TestCsv1.csv");
        temporaryFolder.newFile("TestCsv2.csv");
        temporaryFolder.newFile("TestCsv3.csv");
        temporaryFolder.newFile("TestXls1.xsl");
        temporaryFolder.newFile("TestJson1.json");
    }

    @After
    public void clearDB() throws Exception {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testRemoveListenerWithNotExistingName() {
        try {
            testResult(db, "CALL apoc.load.folder.remove('notExisting') YIELD name, pattern, cypher RETURN name, pattern, cypher", result -> {});
        } catch (RuntimeException e) {
            String expectedMessage = "Listener with name: notExisting doesn't exists";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testFolderListenerList() {
        db.executeTransactionally("CALL apoc.load.folder.add('test','*.csv','CREATE (n:Test)', {}) YIELD name RETURN name");
        db.executeTransactionally("CALL apoc.load.folder.add('testTwo','*.json','CREATE (n:Test)', {}) YIELD name RETURN name");

        testResult(db, "CALL apoc.load.folder.list() YIELD name RETURN name", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("name"));
                    assertEquals(2, rows.size());
                }
        );

        testResult(db, "CALL apoc.load.folder.remove('test') YIELD name RETURN name", result -> {
                    Map<String, Object> row = result.next();
                    assertEquals("test", row.get("name"));
                    assertFalse(result.hasNext());
                }
        );
        testResult(db, "CALL apoc.load.folder.remove('testTwo') YIELD name RETURN name", result -> {
                    Map<String, Object> row = result.next();
                    assertEquals("testTwo", row.get("name"));
                    assertFalse(result.hasNext());
                }
        );

    }

    @Test
    public void testWithFilterAll() {
        testResult(db, "CALL apoc.load.folder('*', {recursive: false}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertEquals(5, rows.size());
                }
        );
    }

    @Test
    public void testWithFilterCsv() {
        testResult(db, "CALL apoc.load.folder('*.csv', {}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertEquals(3, rows.size());
                }
        );
    }

    @Test
    public void testWithRecursiveFalse() throws IOException {
        temporaryFolder.newFile("subfolder/fileSub.csv");

        testResult(db, "CALL apoc.load.folder('*.csv', {recursive: true}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertEquals(4, rows.size());
                }
        );

        testResult(db, "CALL apoc.load.folder('*.csv', {recursive: false}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertEquals(3, rows.size());
                }
        );

        File file = new File(temporaryFolder.getRoot() + "/subfolder/fileSub.csv");
        file.delete();
    }

    @Test
    public void testRemoveFolderListener() {
        db.executeTransactionally("CALL apoc.load.folder.add('test','*.csv','CREATE (n:Test)', {}) YIELD name RETURN name");
        testResult(db, "CALL apoc.load.folder.list() YIELD name RETURN name", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("name"));
                    assertEquals(1, rows.size());
                }
        );
        testResult(db, "CALL apoc.load.folder.remove('test') YIELD name, pattern, cypher RETURN name, pattern, cypher", result -> {
                    Map<String, Object> row = result.next();
                    assertEquals("test", row.get("name"));
                    assertEquals("*.csv", row.get("pattern"));
                    assertEquals("CREATE (n:Test)", row.get("cypher"));
                    assertFalse(result.hasNext());
                }
        );
        testResult(db, "CALL apoc.load.folder.list() YIELD name RETURN name", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("name"));
                    assertEquals(0, rows.size());
                }
        );
    }

    @Test
    public void testFolderListenerNotMatchingPattern() throws IOException, InterruptedException {
        db.executeTransactionally("CALL apoc.load.folder.add('test','*.json','CREATE (n:Test)', {}) YIELD name RETURN name");

        try (Transaction tx = db.beginTx()) {
            assertEquals(0, Iterators.count(tx.findNodes(Label.label("Test"))));
            tx.commit();
        }

        temporaryFolder.newFile("newFile.csv");
        Thread.sleep(15000);

        try (Transaction tx = db.beginTx()) {
            assertEquals(0, Iterators.count(tx.findNodes(Label.label("Test"))));
            tx.commit();
        }

        testResult(db, "CALL apoc.load.folder.remove('test') YIELD name RETURN name", result -> {
                    Map<String, Object> row = result.next();
                    assertEquals("test", row.get("name"));
                    assertFalse(result.hasNext());
                }
        );

        File file = new File(temporaryFolder.getRoot() + "/newFile.csv");
        file.delete();
    }

    @Test
    public void testFolderListenerWithSameName() throws IOException, InterruptedException {
        testCall(db, "CALL apoc.load.folder.add('test','*csv','CREATE (n:Test)', {}) YIELD name RETURN name",
                r -> assertEquals("test", r.get("name"))
        );
        testCall(db, "CALL apoc.load.folder.add('test','*csv','CREATE (n:TestTwo)', {}) YIELD name RETURN name",
                r -> assertEquals("test", r.get("name"))
        );

        temporaryFolder.newFile("newFile.csv");
        Thread.sleep(15000);

        try (Transaction tx = db.beginTx()) {
            assertEquals(0, Iterators.count(tx.findNodes(Label.label("Test"))));
            assertEquals(1, Iterators.count(tx.findNodes(Label.label("TestTwo"))));
            tx.commit();
        }

        testResult(db, "CALL apoc.load.folder.remove('test') YIELD name RETURN name", result -> {
                    Map<String, Object> row = result.next();
                    assertEquals("test", row.get("name"));
                    assertFalse(result.hasNext());
                }
        );

        File file = new File(temporaryFolder.getRoot() + "/newFile.csv");
        file.delete();
    }

    @Test
    public void testAddFolderListener() throws InterruptedException, IOException {
        testCall(db, "CALL apoc.load.folder.add('test','*csv','CREATE (n:Test)', {}) YIELD name RETURN name",
                r -> assertEquals("test", r.get("name"))
        );

        try (Transaction tx = db.beginTx()) {
            assertEquals(0, Iterators.count(tx.findNodes(Label.label("Test"))));
            tx.commit();
        }

        temporaryFolder.newFile("newFile.csv");
        Thread.sleep(15000);

        try (Transaction tx = db.beginTx()) {
            assertEquals(1, Iterators.count(tx.findNodes(Label.label("Test"))));
            tx.commit();
        }

        testResult(db, "CALL apoc.load.folder.remove('test') YIELD name RETURN name", result -> {
                    Map<String, Object> row = result.next();
                    assertEquals("test", row.get("name"));
                    assertFalse(result.hasNext());
                }
        );

        temporaryFolder.newFile("newOtherFile.csv");
        Thread.sleep(15000);

        try (Transaction tx = db.beginTx()) {
            assertEquals(1, Iterators.count(tx.findNodes(Label.label("Test"))));
            tx.commit();
        }

        File file = new File(temporaryFolder.getRoot() + "/newFile.csv");
        file.delete();
        File fileOther = new File(temporaryFolder.getRoot() + "/newOtherFile.csv");
        fileOther.delete();
    }

}
