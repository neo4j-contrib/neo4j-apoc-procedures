package apoc.azure;

import apoc.export.arrow.ExportArrow;
import apoc.export.arrow.ImportArrow;
import apoc.export.arrow.ImportArrowTestUtil;
import apoc.load.Gexf;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import apoc.util.s3.S3BaseTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ImportArrowTestUtil.ARROW_BASE_FOLDER;
import static apoc.export.arrow.ImportArrowTestUtil.MAPPING_ALL;
import static apoc.export.arrow.ImportArrowTestUtil.prepareDbForArrow;
import static apoc.export.arrow.ImportArrowTestUtil.testImportCommon;
import static apoc.util.ExtendedITUtil.EXTENDED_PATH;
import static apoc.util.ExtendedTestUtil.clearDb;
import static apoc.util.GexfTestUtil.testImportGexfCommon;

public class ImportAzureStorageTest extends AzureStorageBaseTest {
    private static File directory = new File(ARROW_BASE_FOLDER);
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        TestUtil.registerProcedure(db, ExportArrow.class, ImportArrow.class, Meta.class, Gexf.class);
        prepareDbForArrow(db);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testImportArrow() {
        String fileWithPath = ARROW_BASE_FOLDER + File.separator + "test_all.arrow";
        String url = putToAzureStorageAndGetUrl(fileWithPath);

        testImportCommon(db, url, MAPPING_ALL);
    }

    @Test
    public void testImportGexf() {
        clearDb(db);
        String filename = EXTENDED_PATH + "src/test/resources/gexf/data.gexf";
        String url = putToAzureStorageAndGetUrl(filename);
        testImportGexfCommon(db, url);
    }
}
