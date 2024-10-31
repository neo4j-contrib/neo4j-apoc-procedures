package apoc.export;

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
import org.neo4j.configuration.GraphDatabaseSettings;
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
import static apoc.util.ExtendedTestUtil.clearDb;
import static apoc.util.GexfTestUtil.testImportGexfCommon;
import static apoc.util.s3.S3Util.putToS3AndGetUrl;

public class ImportS3Test extends S3BaseTest {
    private static File directory = new File(ARROW_BASE_FOLDER);
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void beforeClass() {
        prepareDbForArrow(db);
        TestUtil.registerProcedure(db, ExportArrow.class, ImportArrow.class, Meta.class, Gexf.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testImportArrow() {
        String file = db.executeTransactionally("CALL apoc.export.arrow.all('test_all.arrow') YIELD file",
                Map.of(),
                ImportArrowTestUtil::extractFileName);

        String fileWithPath = ARROW_BASE_FOLDER + File.separator + file;
        String url = putToS3AndGetUrl(s3Container, fileWithPath);

        testImportCommon(db, url, MAPPING_ALL);
    }

    @Test
    public void testImportGexf() {
        clearDb(db);
        String file = Thread.currentThread().getContextClassLoader().getResource("gexf/data.gexf").toString();
        String filename = file.substring(file.indexOf("build"));
        String url = putToS3AndGetUrl(s3Container, filename);
        testImportGexfCommon(db, url);
    }
}
