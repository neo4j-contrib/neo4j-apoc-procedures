package apoc.full.it.util;

import apoc.util.Util;
import apoc.xml.XmlTestUtils;
import org.junit.Assert;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphdb.GraphDatabaseService;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ExtendedITUtil {
    public static String RESOURCES_PATH = "src/test/resources/";
    public static final String EXTENDED_PATH = "../full/";
    public static final String EXTENDED_RESOURCES_PATH = EXTENDED_PATH + RESOURCES_PATH;
    
    public static void testLoadXmlCommon(GraphDatabaseService db, String url) {
        testCall(db, "CALL apoc.load.xml($url,'/catalog/book[title=\"Maeve Ascendant\"]/.',{failOnError:false}) yield value as result", Util.map("url", url), (r) -> {
            Object value = Iterables.single(r.values());
            Assert.assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
        });
    }

    public static void testLoadJsonCommon(GraphDatabaseService db, String url) {
        testCall(db, "CALL apoc.load.jsonParams($url, null, null)",
                map("url", url),
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }
}
