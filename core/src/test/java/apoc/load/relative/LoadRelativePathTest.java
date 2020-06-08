package apoc.load.relative;

import apoc.ApocSettings;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;

import static apoc.load.relative.LoadXmlResult.StringXmlNestedMap;
import static apoc.load.relative.LoadXmlResult.StringXmlNestedSimpleMap;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LoadRelativePathTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true)
            .withSetting(GraphDatabaseSettings.allow_file_urls, true)
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, Path.of(RESOURCE.toURI()).getParent());

    public static final URL RESOURCE = LoadRelativePathTest.class.getClassLoader().getResource("map.json");

    public LoadRelativePathTest() throws URISyntaxException {
    }

    @Before public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadJson.class, Xml.class);
    }

    //JSON
    @Test public void testLoadRelativePathJson() {
        String url = "file:/map.json";
        testCall(db, "CALL apoc.load.json($url)",map("url",url), // 'file:map.json' YIELD value RETURN value
                (row) -> assertEquals(map("foo",asList(1L,2L,3L)), row.get("value")));
    }

    //XML
    @Test
    public void testLoadRelativePathXml() {
        testCall(db, "CALL apoc.load.xml('file:///xml/databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(StringXmlNestedMap(), value);
                });
    }

    @Test
    public void testLoadRelativePathSimple() {
        testCall(db, "CALL apoc.load.xmlSimple('/xml/databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(StringXmlNestedSimpleMap(), value);
                });
    }
}
