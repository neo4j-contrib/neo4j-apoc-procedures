package apoc.load;

import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.util.List;
import java.util.Map;

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

    @Test
    public void testWithFilterAll() {
        testResult(db, "CALL apoc.load.folder('*', {recursive: false}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertTrue(rows.contains(temporaryFolder.getRoot().toString() + "/TestCsv1.csv"));
                    assertTrue(rows.contains(temporaryFolder.getRoot().toString() + "/TestJson1.json"));
                    assertEquals(5, rows.size());
                }
        );
    }

    @Test
    public void testWithFilterCsv() {
        testResult(db, "CALL apoc.load.folder('*.csv', {}) YIELD url RETURN url", result -> {
                    List<Map<String, Object>> rows = Iterators.asList(result.columnAs("url"));
                    assertTrue(rows.contains(temporaryFolder.getRoot().toString() + "/TestCsv1.csv"));
                    assertEquals(3, rows.size());
                }
        );
    }

}
