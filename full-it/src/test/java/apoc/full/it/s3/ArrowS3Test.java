package apoc.full.it.s3;

import apoc.export.arrow.ArrowTestUtil;
import apoc.export.arrow.ImportArrow;
import apoc.util.TestUtil;
import apoc.util.s3.S3BaseTest;
import org.junit.Before;
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

public class ArrowS3Test extends S3BaseTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void beforeClass() {
        initDbCommon(db);
        TestUtil.registerProcedure(db, ImportArrow.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testFileRoundtripWithLoadArrow() {
        String url = s3Container.getUrl("test_all.arrow");
        
        String file = db.executeTransactionally("CALL apoc.export.arrow.all($url) YIELD file",
                Map.of("url", url),
                ArrowTestUtil::extractFileName);

        // check that the exported file is correct
        final String query = "CALL apoc.load.arrow($file, {})";
        testLoadArrow(db, query, Map.of("file", file));
    }


    @Test
    public void testFileRoundtripWithImportArrow() {
        db.executeTransactionally("CREATE (:Another {foo:1, listInt: [1,2]}), (:Another {bar:'Sam'})");

        String url = s3Container.getUrl("test_all_import.arrow");
        String file = db.executeTransactionally("CALL apoc.export.arrow.all($url) YIELD file",
                Map.of("url", url),
                ArrowTestUtil::extractFileName);
        
        // check that the exported file is correct
        testImportCommon(db, file, ArrowTestUtil.MAPPING_ALL);
    }
}
