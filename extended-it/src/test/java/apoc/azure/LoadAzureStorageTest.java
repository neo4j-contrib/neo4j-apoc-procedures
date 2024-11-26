package apoc.azure;

import apoc.load.LoadCsv;
import apoc.load.LoadDirectory;
import apoc.load.LoadHtml;
import apoc.load.LoadJson;
import apoc.load.LoadJsonExtended;
import apoc.load.Xml;
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
import static apoc.load.xls.LoadXlsTest.testLoadXlsCommon;
import static apoc.util.ExtendedITUtil.EXTENDED_PATH;
import static apoc.util.ExtendedITUtil.testLoadJsonCommon;
import static apoc.util.ExtendedITUtil.testLoadXmlCommon;


public class LoadAzureStorageTest extends AzureStorageBaseTest {

    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);
    
    @BeforeClass
    public static void setUp() throws Exception {
        AzureStorageBaseTest.setUp();
        
        TestUtil.registerProcedure(db, LoadCsv.class, LoadDirectory.class, LoadJsonExtended.class, LoadHtml.class, LoadXls.class, Xml.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
    }


    @Test
    public void testLoadCsv() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_PATH + "src/test/resources/test.csv");
        commonTestLoadCsv(db, url);
    }

    @Test
    public void testLoadJson() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_PATH + "src/test/resources/map.json");
        testLoadJsonCommon(db, url);
    }

    @Test
    public void testLoadXml() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_PATH + "src/test/resources/xml/books.xml");
        testLoadXmlCommon(db, url);
    }

    @Test
    public void testLoadXls() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_PATH + "src/test/resources/load_test.xlsx");
        testLoadXlsCommon(db, url);
    }

    @Test
    public void testLoadHtml() {
        String url = putToAzureStorageAndGetUrl(EXTENDED_PATH + "src/test/resources/wikipedia.html");
        testLoadHtmlWithGetLinksCommon(db, url);
    }

}
