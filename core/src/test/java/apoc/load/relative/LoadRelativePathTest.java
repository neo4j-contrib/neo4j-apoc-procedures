package apoc.load.relative;

import apoc.ApocConfig;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.load.relative.LoadXmlResult.StringXmlNestedMap;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LoadRelativePathTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.allow_file_urls, true)
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, Path.of(RESOURCE.toURI()).getParent());

    public static final URL RESOURCE = LoadRelativePathTest.class.getClassLoader().getResource("map.json");

    public LoadRelativePathTest() throws URISyntaxException {
    }

    @Before public void setUp() throws Exception {
        ApocConfig apocConfig = ApocConfig.apocConfig();
        apocConfig.setProperty( APOC_IMPORT_FILE_ENABLED, true );
        TestUtil.registerProcedure(db, LoadJson.class, Xml.class);
    }

    @After
    public void tearDown() throws Exception
    {
        ApocConfig apocConfig = ApocConfig.apocConfig();
        apocConfig.setProperty( APOC_IMPORT_FILE_ENABLED, false );
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
}
