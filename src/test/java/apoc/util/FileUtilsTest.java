package apoc.util;

import apoc.ApocSettings;
import apoc.config.Config;
import apoc.load.relative.LoadRelativePathTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class FileUtilsTest {

    @Rule
    public TestName testName = new TestName();

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.allow_file_urls, "true")
            .withSetting(ApocSettings.dynamic("foo", SettingValueParsers.STRING), "BAR")
            ;

    //TODO: conditionally amend import folder to settings
    /*
            if (testName.getMethodName().endsWith(TEST_WITH_DIRECTORY_IMPORT)) {
            builder.setConfig("dbms.directories.import", "import");
        }

     */

    private static final File PATH = new File("target/test-data");

    private static String TEST_FILE_RELATIVE = new File(PATH.getAbsolutePath() + "/import/test.csv").toURI().toString();
    private static String TEST_FILE_ABSOLUTE = new File(LoadRelativePathTest.class.getClassLoader().getResource("test.csv").getPath()).toURI().toString();
    private static String TEST_FILE = LoadRelativePathTest.class.getClassLoader().getResource("test.csv").getPath();

    private static final String TEST_WITH_DIRECTORY_IMPORT = "WithDirectoryImport";

    @Before
    public void setUp() throws Exception {
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
