package apoc.full.it.gc;

import apoc.load.Gexf;
import apoc.util.GoogleCloudStorageContainerExtension;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
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
import static apoc.util.GexfTestUtil.testImportGexfCommon;
import static apoc.util.GoogleCloudStorageContainerExtension.gcsUrl;

public class ImportGoogleCloudStorageTest {
    public static GoogleCloudStorageContainerExtension gcs = new GoogleCloudStorageContainerExtension()
            .withMountedResourceFile("test_all.arrow", "/folder/test_all.arrow")
            .withMountedResourceFile("gexf/data.gexf", "/folder/data.gexf");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        
        gcs.start();
    }

    @Test
    public void testImportArrow() {
        initDbCommon(db);
        createNodesForImportTests(db);
        
        String url = gcsUrl(gcs, "test_all.arrow");
        testImportCommon(db, url, MAPPING_ALL);
    }

}
