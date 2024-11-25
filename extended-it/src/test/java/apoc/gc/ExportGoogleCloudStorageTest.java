package apoc.gc;

import apoc.export.arrow.ArrowTestUtil;
import apoc.util.GoogleCloudStorageContainerExtension;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.export.arrow.ArrowTestUtil.beforeClassCommon;
import static apoc.export.arrow.ArrowTestUtil.createNodesForImportTests;
import static apoc.export.arrow.ArrowTestUtil.testImportCommon;
import static apoc.export.csv.ExportXlsTest.assertResults;
import static apoc.util.GoogleCloudStorageContainerExtension.gcsUrl;
import static apoc.util.MapUtil.map;

public class ExportGoogleCloudStorageTest {
    public static final String TEST_BUCKET = "test-bucket";
    public static GoogleCloudStorageContainerExtension gcs = new GoogleCloudStorageContainerExtension();

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);

    @BeforeClass
    public static void setUp() throws Exception {
        gcs.start();
        gcs.createBucket(TEST_BUCKET);

        beforeClassCommon(db);
        createNodesForImportTests(db);
    }

    @Test
    public void testExportArrow() {
        String url = gcs.gcsUrl(TEST_BUCKET, "test_all.arrow");

        String file = db.executeTransactionally("CYPHER 25 CALL apoc.export.arrow.all($url) YIELD file",
                Map.of("url", url),
                ArrowTestUtil::extractFileName);

        testImportCommon(db, file, ArrowTestUtil.MAPPING_ALL);
    }

    @Test
    public void testExportXls() {
        String url = gcsUrl(gcs, "export.xls");
        System.out.println("url = " + url);
        TestUtil.testCall(db, "CALL apoc.export.xls.all($file,null)",
                map("file", url),
                (r) -> assertResults(url, r, "database"));
    }

}
