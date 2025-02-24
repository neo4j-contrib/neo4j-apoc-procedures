package apoc.full.it.gc;

import static apoc.full.it.util.ExtendedITUtil.testLoadJsonCommon;
import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.GoogleCloudStorageContainerExtension.gcsUrl;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import apoc.load.LoadCsv;
import apoc.load.LoadHtml;
import apoc.load.LoadJson;
import apoc.load.LoadXls;
import apoc.load.Xml;
import apoc.util.GoogleCloudStorageContainerExtension;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.xml.XmlTestUtils;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class LoadGoogleCloudStorageTest {

    public static GoogleCloudStorageContainerExtension gcs = new GoogleCloudStorageContainerExtension()
            .withMountedResourceFile("test.csv", "/folder/test.csv")
            .withMountedResourceFile("map.json", "/folder/map.json")
            .withMountedResourceFile("xml/books.xml", "/folder/books.xml")
            .withMountedResourceFile("load_test.xlsx", "/folder/load_test.xlsx")
            .withMountedResourceFile("wikipedia.html", "/folder/wikipedia.html");

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        gcs.start();
        TestUtil.registerProcedure(db, LoadCsv.class, LoadJson.class, LoadHtml.class, LoadXls.class, Xml.class);
    }

    @AfterClass
    public static void tearDown() {
        gcs.close();
        db.shutdown();
    }

    @Test
    public void testLoadCsv() {
        String url = gcsUrl(gcs, "test.csv");

        testResult(db, "CALL apoc.load.csv($url)", map("url", url), (r) -> {
            assertRow(r, "Selma", "8", 0L);
            assertRow(r, "Rana", "11", 1L);
            assertRow(r, "Selina", "18", 2L);
            assertFalse("It should be the last record", r.hasNext());
        });
    }

    @Test
    public void testLoadJSON() {
        String url = gcsUrl(gcs, "map.json");
        testLoadJsonCommon(db, url);
    }

    @Test
    public void testLoadXml() {
        String url = gcsUrl(gcs, "books.xml");
        testCall(
                db,
                "CALL apoc.load.xml($url,'/catalog/book[title=\"Maeve Ascendant\"]/.',{failOnError:false}) yield value as result",
                Util.map("url", url),
                (r) -> {
                    Object value = Iterables.single(r.values());
                    Assert.assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
                });
    }

    @Test
    public void testLoadXls() {
        String url = gcsUrl(gcs, "load_test.xlsx");
        testResult(
                db,
                "CALL apoc.load.xls($url,'Full',{mapping:{Integer:{type:'int'}, Array:{type:'int',array:true,arraySep:';'}}})",
                map("url", url), // 'file:load_test.xlsx'
                (r) -> {
                    assertXlsRow(
                            r,
                            0L,
                            "String",
                            "Test",
                            "Boolean",
                            true,
                            "Integer",
                            2L,
                            "Float",
                            1.5d,
                            "Array",
                            asList(1L, 2L, 3L));
                    assertFalse("Should not have another row", r.hasNext());
                });
    }

    @Test
    public void testLoadHtml() {
        String url = gcsUrl(gcs, "wikipedia.html");

        Map<String, Object> query = map("links", "a[href]");

        testCall(db, "CALL apoc.load.html($url,$query)", map("url", url, "query", query), row -> {
            final List<Map<String, Object>> actual = (List) ((Map) row.get("value")).get("links");
            assertEquals(106, actual.size());
            assertTrue(actual.stream().allMatch(i -> i.get("tagName").equals("a")));
        });
    }

    static void assertXlsRow(Result r, long lineNo, Object... data) {
        Map<String, Object> row = r.next();
        Map<String, Object> map = map(data);
        assertEquals(map, row.get("map"));
        Map<Object, Object> stringMap = new LinkedHashMap<>(map.size());
        map.forEach((k, v) -> stringMap.put(k, v == null ? null : v.toString()));
        assertEquals(new ArrayList<>(map.values()), row.get("list"));
        assertEquals(lineNo, row.get("lineNo"));
    }
}
