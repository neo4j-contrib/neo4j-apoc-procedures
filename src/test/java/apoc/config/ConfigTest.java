package apoc.config;

import apoc.ApocConfiguration;
import apoc.export.util.FileUtils;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 28.10.16
 */
public class ConfigTest {

    private GraphDatabaseService db;
    private File testFile;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.use_neo4j_config", "true")
                .setConfig("dbms.directories.import", "build/resources/test")
                .setConfig("dbms.security.allow_csv_import_from_file_urls","true")
                .setConfig("foo", "bar").setConfig("foouri", "baruri")
                .setConfig("foopass", "foopass").setConfig("foo.credentials", "foo.credentials").newGraphDatabase();
        TestUtil.registerProcedure(db, Config.class);
        testFile = new File(ClassLoader.getSystemResource("test.csv").getPath());
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    @Test
    public void list() throws Exception {
        TestUtil.testCall(db, "CALL apoc.config.list() yield key with * where key STARTS WITH 'foo' RETURN *",(row) -> assertEquals("foo",row.get("key")));
    }

    @Test
    public void getConfigurationValue(){
        assertEquals(ApocConfiguration.list().get("foo"),"bar");
    }

    @Test
    public void changeNoSlashesUrlWithDirectoryImportContrained() throws Exception {
        assertEquals(new URL("file","",testFile.getAbsolutePath()).toString(), FileUtils.changeFileUrlIfImportDirectoryConstrained("test.csv"));
    }

    @Test
    public void changeSlashUrlWithDirectoryImportContrained() throws Exception {
        assertEquals(new URL("file","",testFile.getAbsolutePath()).toString(), FileUtils.changeFileUrlIfImportDirectoryConstrained("/test.csv"));
    }

    @Test
    public void changeFileSlashUrlWithDirectoryImportContrained() throws Exception {

        assertEquals(new URL("file","",testFile.getAbsolutePath()).toString(), FileUtils.changeFileUrlIfImportDirectoryConstrained("file:/test.csv"));
    }

    @Test
    public void changeFileDoubleSlashesUrlWithdirectoryImportConstrained() throws Exception {
        assertEquals(new URL("file","",testFile.getAbsolutePath()).toString(), FileUtils.changeFileUrlIfImportDirectoryConstrained("file://test.csv"));
    }

    @Test
    public void changeFileTripleSlashesUrlWithdirectoryImportConstrained() throws Exception {
        assertEquals(new URL("file","",testFile.getAbsolutePath()).toString(), FileUtils.changeFileUrlIfImportDirectoryConstrained("file:///test.csv"));
    }

    @Test
    public void notChangeFileUrlWithdirectoryImportConstrained() throws Exception {
        assertEquals(new URL("file","",testFile.getAbsolutePath()).toString(), FileUtils.changeFileUrlIfImportDirectoryConstrained("build/resources/test/test.csv"));
    }

}
