package apoc.azure;

import apoc.load.LoadCsv;
import apoc.load.LoadDirectory;
import apoc.load.LoadHtml;
import apoc.load.LoadJsonExtended;
import apoc.load.Xml;
import apoc.load.partial.LoadPartial;
import apoc.load.xls.LoadXls;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.load.LoadCsvTest.commonTestLoadCsv;
import static apoc.load.LoadHtmlTest.testLoadHtmlWithGetLinksCommon;
import static apoc.load.partial.LoadPartialTest.PARTIAL_CSV;
import static apoc.load.xls.LoadXlsTest.testLoadXlsCommon;
import static apoc.util.ExtendedITUtil.EXTENDED_RESOURCES_PATH;
import static apoc.util.ExtendedITUtil.testLoadJsonCommon;
import static apoc.util.ExtendedITUtil.testLoadXmlCommon;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.singleResultFirstColumn;
import static apoc.util.s3.S3Util.putToS3AndGetUrl;
import static org.junit.Assert.assertEquals;


public class LoadAzureStorageTest extends AzureStorageBaseTest {
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);
    
    @BeforeClass
    public static void setUp() throws Exception {
        AzureStorageBaseTest.setUp();
        
        TestUtil.registerProcedure(db, LoadCsv.class, LoadDirectory.class, LoadJsonExtended.class, LoadHtml.class, LoadXls.class, Xml.class, LoadPartial.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
    }


    @Test
    public void testLoadCsv() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_RESOURCES_PATH + "test.csv");
        commonTestLoadCsv(db, url);
    }

    @Test
    public void testLoadJson() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_RESOURCES_PATH + "map.json");
        testLoadJsonCommon(db, url);
    }

    @Test
    public void testLoadXml() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_RESOURCES_PATH + "xml/books.xml");
        testLoadXmlCommon(db, url);
    }

    @Test
    public void testLoadXls() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_RESOURCES_PATH + "load_test.xlsx");
        testLoadXlsCommon(db, url);
    }

    @Test
    public void testLoadHtml() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_RESOURCES_PATH + "wikipedia.html");
        testLoadHtmlWithGetLinksCommon(db, url);
    }

    @Test
    public void testLoadPartial() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_RESOURCES_PATH + "test.csv");
        String result = singleResultFirstColumn(db, "CALL apoc.load.stringPartial($url, 17, 15)",
                map("url", url)
        );

        assertEquals(PARTIAL_CSV, result);
    }

}
