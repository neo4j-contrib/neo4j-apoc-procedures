package apoc.azure;

import apoc.load.Gexf;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowTestUtil.ARROW_BASE_FOLDER;
import static apoc.export.arrow.ArrowTestUtil.MAPPING_ALL;
import static apoc.export.arrow.ArrowTestUtil.beforeClassCommon;
import static apoc.export.arrow.ArrowTestUtil.createNodesForImportTests;
import static apoc.export.arrow.ArrowTestUtil.testImportCommon;
import static apoc.util.ExtendedITUtil.EXTENDED_PATH;
import static apoc.util.GexfTestUtil.testImportGexfCommon;

public class ImportAzureStorageTest extends AzureStorageBaseTest {
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        
        // for arrow test
        beforeClassCommon(db);
        createNodesForImportTests(db);

        // for gexf test
        TestUtil.registerProcedure(db, Gexf.class);
    }

    @Test
    public void testImportArrow() {
        String fileWithPath = EXTENDED_PATH + ARROW_BASE_FOLDER + File.separator + "test_all.arrow";
        String url = putToAzureStorageAndGetUrl(fileWithPath);

        testImportCommon(db, url, MAPPING_ALL);
    }

    @Test
    public void testImportGexf() {
        String filename = EXTENDED_PATH + "src/test/resources/gexf/data.gexf";
        String url = putToAzureStorageAndGetUrl(filename);
        testImportGexfCommon(db, url);
    }
}
