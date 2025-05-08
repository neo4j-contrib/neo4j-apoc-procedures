package apoc.azure;

import apoc.export.parquet.ParquetTestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.export.parquet.ParquetTest.MAPPING_ALL;
import static apoc.export.parquet.ParquetTestUtil.beforeClassCommon;
import static apoc.export.parquet.ParquetTestUtil.beforeCommon;
import static apoc.export.parquet.ParquetTestUtil.testImportAllCommon;
import static apoc.util.ExtendedITUtil.EXTENDED_RESOURCES_PATH;
import static apoc.util.GoogleCloudStorageContainerExtension.gcsUrl;
import static apoc.util.TestUtil.testResult;

public class ParquetAzureStorageTest extends AzureStorageBaseTest {

    private final String EXPORT_FILENAME = "test_all.parquet";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        beforeClassCommon(db);
    }

    @Before
    public void before() {
        beforeCommon(db);
    }
    
    @Test
    @Ignore("This test won't work until the Azure Storage files will be correctly handled via FileUtils, placed in APOC Core")
    public void testFileRoundtripParquetAll() {
        // given - when
        String url = putToAzureStorageAndGetUrl(EXTENDED_RESOURCES_PATH + EXPORT_FILENAME);
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

        String url = putToAzureStorageAndGetUrl(EXTENDED_RESOURCES_PATH + EXPORT_FILENAME);
        testResult(db, query, Map.of("url", url,  "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);
    }

    @Test
    public void testImportParquet() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_RESOURCES_PATH + EXPORT_FILENAME);

        Map<String, Object> params = Map.of("file", url, "config", MAPPING_ALL);
        testImportAllCommon(db, params);
    }
}
