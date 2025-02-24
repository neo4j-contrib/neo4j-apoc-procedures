package apoc.full.it.s3;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowTestUtil.ARROW_BASE_FOLDER;
import static apoc.export.arrow.ArrowTestUtil.MAPPING_ALL;
import static apoc.export.arrow.ArrowTestUtil.createNodesForImportTests;
import static apoc.export.arrow.ArrowTestUtil.initDbCommon;
import static apoc.export.arrow.ArrowTestUtil.testImportCommon;
import static apoc.full.it.util.ExtendedITUtil.EXTENDED_RESOURCES_PATH;
import static apoc.util.s3.S3TestUtil.putToS3AndGetUrl;

import apoc.export.arrow.ImportArrow;
import apoc.util.TestUtil;
import apoc.util.s3.S3BaseTest;
import java.io.File;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class ImportS3Test extends S3BaseTest {
    private static File directory = new File(ARROW_BASE_FOLDER);

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(
                    GraphDatabaseSettings.load_csv_file_url_root,
                    directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void beforeClass() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testImportArrow() {
        initDbCommon(db);
        TestUtil.registerProcedure(db, ImportArrow.class);
        createNodesForImportTests(db);

        String fileWithPath = EXTENDED_RESOURCES_PATH + "test_all.arrow";
        String url = putToS3AndGetUrl(s3Container, fileWithPath);

        testImportCommon(db, url, MAPPING_ALL);
    }
}
