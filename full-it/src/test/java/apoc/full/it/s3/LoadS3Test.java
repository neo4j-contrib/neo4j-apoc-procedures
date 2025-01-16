package apoc.full.it.s3;

import apoc.load.LoadCsv;
import apoc.load.LoadDirectory;
import apoc.load.LoadHtml;
import apoc.load.LoadJson;
import apoc.load.LoadXls;
import apoc.load.Xml;
import apoc.util.TestUtil;
import apoc.util.s3.S3BaseTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.full.it.util.ExtendedITUtil.EXTENDED_RESOURCES_PATH;
import static apoc.full.it.util.ExtendedITUtil.testLoadJsonCommon;
import static apoc.full.it.util.ExtendedITUtil.testLoadXmlCommon;
import static apoc.load.LoadCsvTest.commonTestLoadCsv;
import static apoc.load.LoadHtmlTest.testLoadHtmlWithGetLinksCommon;
import static apoc.load.LoadXlsTest.testLoadXlsCommon;
import static apoc.util.s3.S3TestUtil.putToS3AndGetUrl;

public class LoadS3Test extends S3BaseTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, LoadDirectory.class, LoadJson.class, LoadHtml.class, LoadXls.class, Xml.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
    }

    @Test
    public void testLoadCsv() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_RESOURCES_PATH + "test.csv");
        commonTestLoadCsv(db, url);
    }

    @Test
    public void testLoadJson() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_RESOURCES_PATH + "map.json");
        testLoadJsonCommon(db, url);
    }

    @Test
    public void testLoadXml() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_RESOURCES_PATH + "xml/books.xml");
        testLoadXmlCommon(db, url);
    }

    @Test
    public void testLoadXls() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_RESOURCES_PATH + "load_test.xlsx");
        testLoadXlsCommon(db, url);
    }

    @Test
    public void testLoadHtml() {
        String url = putToS3AndGetUrl(s3Container, EXTENDED_RESOURCES_PATH + "wikipedia.html");
        testLoadHtmlWithGetLinksCommon(db, url);
    }

}
