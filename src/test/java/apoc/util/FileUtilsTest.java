package apoc.util;

import apoc.config.Config;
import apoc.load.relative.LoadRelativePathTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class FileUtilsTest {
    private GraphDatabaseService db;
    private static final File PATH = new File("target/test-data/impermanent-db");

    private static String TEST_FILE_RELATIVE = new File(PATH.getAbsolutePath() + "/import/test.csv").toURI().toString();
    private static String TEST_FILE_ABSOLUTE = new File(LoadRelativePathTest.class.getClassLoader().getResource("test.csv").getPath()).toURI().toString();
    private static String TEST_FILE = LoadRelativePathTest.class.getClassLoader().getResource("test.csv").getPath();

    @Rule
    public TestName testName = new TestName();

    private static final String TEST_WITH_DIRECTORY_IMPORT = "WithDirectoryImport";

    @Before
    public void setUp() throws Exception {
        GraphDatabaseBuilder builder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder(PATH)
                .setConfig("dbms.security.allow_csv_import_from_file_urls", "true")
                .setConfig("foo", "BAR");
        if (testName.getMethodName().endsWith(TEST_WITH_DIRECTORY_IMPORT)) {
            builder.setConfig("dbms.directories.import", "import");
        }
        db = builder.newGraphDatabase();
        TestUtil.registerProcedure(db, Config.class);
    }

    @Test
    public void notChangeFileUrlWithDirectoryImportConstrainedURI() throws Exception {
        assertEquals(TEST_FILE_ABSOLUTE, FileUtils.changeFileUrlIfImportDirectoryConstrained(TEST_FILE));
    }

    @Test
    public void notChangeFileUrlWithDirectoryAndProtocolImportConstrainedURI() throws Exception {
        assertEquals(TEST_FILE_ABSOLUTE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file:///" + TEST_FILE));
    }

    @Test
    public void changeNoSlashesUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("test.csv"));
    }

    @Test
    public void changeSlashUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("/test.csv"));
    }

    @Test
    public void changeFileSlashUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file:/test.csv"));
    }

    @Test
    public void changeFileDoubleSlashesUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file://test.csv"));
    }

    @Test
    public void changeFileTripleSlashesUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file:///test.csv"));
    }

    @Test
    public void importDirectoryWithRelativePathWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("test.csv"));
    }

}