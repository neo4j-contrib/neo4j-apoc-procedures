package apoc.gc;

import apoc.export.parquet.ParquetTestUtil;
import apoc.util.GoogleCloudStorageContainerExtension;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.parquet.ParquetTest.MAPPING_ALL;
import static apoc.export.parquet.ParquetTestUtil.beforeClassCommon;
import static apoc.export.parquet.ParquetTestUtil.testImportAllCommon;
import static apoc.util.GoogleCloudStorageContainerExtension.gcsUrl;
import static apoc.util.TestUtil.testResult;

public class ParquetGoogleCloudStorageTest {

    private static final String FILENAME = "test_all.parquet";
    
    public static GoogleCloudStorageContainerExtension gcs;


    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        
        beforeClassCommon(db);

        gcs = new GoogleCloudStorageContainerExtension()
                .withMountedResourceFile(FILENAME, "/folder/" + FILENAME);
        
        gcs.start();
    }

    @AfterClass
    public static void tearDown() {
        gcs.close();
        db.shutdown();
    }

    @Test
    @Ignore("This test won't work until the Google Cloud files will be correctly handled via FileUtils, placed in APOC Core")
    public void testFileRoundtripParquetAll() {
        // given - when
        String url = gcsUrl(gcs, FILENAME);
        String file = db.executeTransactionally("CALL apoc.export.parquet.all($url) YIELD file",
                Map.of("url", url),
                ParquetTestUtil::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("file", file,  "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);
    }

    @Test
    public void testLoadParquet() {
        String query = "CALL apoc.load.parquet($url, $config) YIELD value " +
                "RETURN value";

        String url = gcsUrl(gcs, FILENAME);
        testResult(db, query, Map.of("url", url,  "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);
    }

    @Test
    public void testImportParquet() {
        String url = gcsUrl(gcs, FILENAME);

        Map<String, Object> params = Map.of("file", url, "config", MAPPING_ALL);
        testImportAllCommon(db, params);
    }
}
