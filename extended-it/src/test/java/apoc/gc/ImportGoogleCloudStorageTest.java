package apoc.gc;

import apoc.export.arrow.ExportArrow;
import apoc.export.arrow.ImportArrow;
import apoc.load.Gexf;
import apoc.meta.Meta;
import apoc.util.GoogleCloudStorageContainerExtension;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ImportArrowTestUtil.MAPPING_ALL;
import static apoc.export.arrow.ImportArrowTestUtil.prepareDbForArrow;
import static apoc.export.arrow.ImportArrowTestUtil.testImportCommon;
import static apoc.util.ExtendedTestUtil.clearDb;
import static apoc.util.GexfTestUtil.testImportGexfCommon;
import static apoc.util.GoogleCloudStorageContainerExtension.gcsUrl;

public class ImportGoogleCloudStorageTest {
    public static GoogleCloudStorageContainerExtension gcs = new GoogleCloudStorageContainerExtension()
            .withMountedResourceFile("test_all.arrow", "/folder/test_all.arrow")
            .withMountedResourceFile("gexf/data.gexf", "/folder/data.gexf");

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        gcs.start();
        TestUtil.registerProcedure(db, ExportArrow.class, ImportArrow.class, Meta.class, Gexf.class);
        prepareDbForArrow(db);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testImportArrow() {
        String url = gcsUrl(gcs, "test_all.arrow");
        testImportCommon(db, url, MAPPING_ALL);
    }

    @Test
    public void testImportGexf() {
        clearDb(db);
        String url = gcsUrl(gcs, "data.gexf");
        testImportGexfCommon(db, url);
    }

}
