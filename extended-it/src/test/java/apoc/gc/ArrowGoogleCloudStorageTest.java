package apoc.gc;

import apoc.export.arrow.ArrowTestUtil;
import apoc.util.GoogleCloudStorageContainerExtension;
import apoc.util.s3.S3BaseTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowTestUtil.initDbCommon;
import static apoc.export.arrow.ArrowTestUtil.testImportCommon;
import static apoc.export.arrow.ArrowTestUtil.testLoadArrow;
import static apoc.util.GoogleCloudStorageContainerExtension.gcsUrl;

@Ignore("This test won't work until the Google Cloud files will be correctly handled via FileUtils, placed in APOC Core")
public class ArrowGoogleCloudStorageTest {
    public static GoogleCloudStorageContainerExtension gcs = new GoogleCloudStorageContainerExtension();

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);

    @Before
    public void beforeClass() {
        initDbCommon(db);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testFileRoundtripWithLoadArrow() {
        String url = gcsUrl(gcs, "test_all.arrow");
        
        String file = db.executeTransactionally("CYPHER 25 CALL apoc.export.arrow.all($url) YIELD file",
                Map.of("url", url),
                ArrowTestUtil::extractFileName);

        // check that the exported file is correct
        final String query = "CYPHER 25 CALL apoc.load.arrow($file, {})";
        testLoadArrow(db, query, Map.of("file", file));
    }


    @Test
    public void testFileRoundtripWithImportArrow() {
        db.executeTransactionally("CREATE (:Another {foo:1, listInt: [1,2]}), (:Another {bar:'Sam'})");

        String url = gcsUrl(gcs, "test_all_import.arrow");
        String file = db.executeTransactionally("CYPHER 25 CALL apoc.export.arrow.all($url) YIELD file",
                Map.of("url", url),
                ArrowTestUtil::extractFileName);
        
        // check that the exported file is correct
        testImportCommon(db, file, ArrowTestUtil.MAPPING_ALL);
    }
}