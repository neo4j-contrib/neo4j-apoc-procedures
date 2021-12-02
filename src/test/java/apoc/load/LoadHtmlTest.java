package apoc.load;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadHtmlTest {

    public static final String WIKIPEDIA_FILE = new File("src/test/resources/wikipedia.html").toURI().toString();
    private GraphDatabaseService db;

    private static final String RESULT_QUERY_METADATA = ("{attributes={charset=UTF-8}, tagName=meta}, " +
            "{attributes={name=ResourceLoaderDynamicStyles}, tagName=meta}, " +
            "{attributes={name=generator, content=MediaWiki 1.32.0-wmf.18}, tagName=meta}, " +
            "{attributes={name=referrer, content=origin}, tagName=meta}, " +
            "{attributes={name=referrer, content=origin-when-crossorigin}, tagName=meta}, " +
            "{attributes={name=referrer, content=origin-when-cross-origin}, tagName=meta}, " +
            "{attributes={property=og:image, content=https://upload.wikimedia.org/wikipedia/en/e/ea/Aap_Kaa_Hak_titles.jpg}, tagName=meta}");

    private static final String RESULT_QUERY_H2 = ("{text=Contents, tagName=h2}, " +
            "{text=Origins[edit], tagName=h2}, " +
            "{text=Content[edit], tagName=h2}, " +
            "{text=Legacy[edit], tagName=h2}, " +
            "{text=References[edit], tagName=h2}, " +
            "{text=Navigation menu, tagName=h2}");

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.enabled","true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, LoadHtml.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testQueryAll(){

        Map<String, Object> query = map("metadata", "meta", "h2", "h2");

        testResult(db, "CALL apoc.load.html({url},{query}, {config})", map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", Collections.emptyMap()),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("metadata",asList(RESULT_QUERY_METADATA)).toString().trim(), row.get("value").toString().trim());
                    row = result.next();
                    assertEquals(map("h2",asList(RESULT_QUERY_H2)).toString().trim(), row.get("value").toString().trim());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryMetadata(){

        Map<String, Object> query = map("metadata", "meta");

        testResult(db, "CALL apoc.load.html({url},{query})", map("url", WIKIPEDIA_FILE, "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("metadata",asList(RESULT_QUERY_METADATA)).toString().trim(), row.get("value").toString().trim());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryH2(){

        Map<String, Object> query = map("h2", "h2");

        testResult(db, "CALL apoc.load.html({url},{query})", map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("h2",asList(RESULT_QUERY_H2)).toString().trim(), row.get("value").toString().trim());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryH2WithConfig(){

        Map<String, Object> query = map("h2", "h2");
        Map<String, Object> config = map("charset", "UTF-8", "baserUri", "");

        testResult(db, "CALL apoc.load.html({url},{query}, {config})", map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", config),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("h2",asList(RESULT_QUERY_H2)).toString().trim(), row.get("value").toString().trim());
                    assertFalse(result.hasNext());
                });
    }
}