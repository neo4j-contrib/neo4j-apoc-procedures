package apoc.s3;

import apoc.load.LoadCsv;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.s3.S3BaseTest;
import apoc.xml.XmlTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;

import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.ExtendedITUtil.EXTENDED_PATH;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadS3Test extends S3BaseTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, LoadJson.class, Xml.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
    }

    @AfterAll
    public void tearDownAll() {
        db.shutdown();
    }

    @Test
    public void testLoadCsvS3() {
        String url = s3Container.putFile(EXTENDED_PATH +  "src/test/resources/test.csv");
        url = removeRegionFromUrl(url);
        
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url", url), (r) -> {
            assertRow(r, "Selma", "8", 0L);
            assertRow(r, "Rana", "11", 1L);
            assertRow(r, "Selina", "18", 2L);
            assertFalse(r.hasNext());
        });
    }

    @Test public void testLoadJsonS3() {
        String url = s3Container.putFile(EXTENDED_PATH +  "src/test/resources/map.json");
        url = removeRegionFromUrl(url);

        testCall(db, "CALL apoc.load.json($url,'')",map("url", url),
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }

    @Test public void testLoadXmlS3() {
        String url = s3Container.putFile(EXTENDED_PATH +  "src/test/resources/xml/books.xml");
        url = removeRegionFromUrl(url);

        testCall(db, "CALL apoc.load.xml($url,'/catalog/book[title=\"Maeve Ascendant\"]/.',{failOnError:false}) yield value as result", Util.map("url", url), (r) -> {
            Object value = Iterables.single(r.values());
            Assert.assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
        });
    }

    private String removeRegionFromUrl(String url) {
        return url.replace(s3Container.getEndpointConfiguration().getSigningRegion() + ".", "");
    }


}
