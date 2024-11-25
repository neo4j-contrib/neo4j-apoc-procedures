package apoc.azure;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;

import static apoc.export.arrow.ArrowTestUtil.ARROW_BASE_FOLDER;
import static apoc.export.arrow.ArrowTestUtil.MAPPING_ALL;
import static apoc.export.arrow.ArrowTestUtil.beforeClassCommon;
import static apoc.export.arrow.ArrowTestUtil.createNodesForImportTests;
import static apoc.export.arrow.ArrowTestUtil.testImportCommon;
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
        beforeClassCommon(db);
        createNodesForImportTests(db);
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
