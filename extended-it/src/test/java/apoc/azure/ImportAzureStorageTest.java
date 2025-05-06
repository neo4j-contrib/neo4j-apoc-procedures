package apoc.azure;

import apoc.load.Gexf;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowTestUtil.MAPPING_ALL;
import static apoc.export.arrow.ArrowTestUtil.initDbCommon;
import static apoc.export.arrow.ArrowTestUtil.createNodesForImportTests;
import static apoc.export.arrow.ArrowTestUtil.testImportCommon;
import static apoc.util.ExtendedITUtil.EXTENDED_RESOURCES_PATH;
import static apoc.util.GexfTestUtil.testImportGexfCommon;

public class ImportAzureStorageTest extends AzureStorageBaseTest {
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void beforeClass() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testImportArrow() {
        initDbCommon(db);
        createNodesForImportTests(db);
        
        String fileWithPath = EXTENDED_RESOURCES_PATH + "test_all.arrow";
        String url = putToAzureStorageAndGetUrl(fileWithPath);

        testImportCommon(db, url, MAPPING_ALL);
    }

    @Test
    public void testImportGexf() {
        TestUtil.registerProcedure(db, Gexf.class);
        
        String filename = EXTENDED_RESOURCES_PATH + "gexf/data.gexf";
        String url = putToAzureStorageAndGetUrl(filename);
        testImportGexfCommon(db, url);
    }
}
