package apoc.load;

import apoc.util.TestUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadHtmlTest {

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

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, LoadHtml.class);
    }

    @Test
    public void testQueryAll(){
        Map<String, Object> query = map("metadata", "meta", "h2", "h2");

        testResult(db, "CALL apoc.load.html($url,$query, $config)", map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", Collections.emptyMap()),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");

                    List<Map<String, Object>> metadata = (List<Map<String, Object>>) value.get("metadata");
                    List<Map<String, Object>> h2 = (List<Map<String, Object>>) value.get("h2");

                    assertEquals(asList(RESULT_QUERY_METADATA).toString().trim(), metadata.toString().trim());
                    assertEquals(asList(RESULT_QUERY_H2).toString().trim(), h2.toString().trim());
                });
    }

    @Test
    public void testQueryMetadata(){
        Map<String, Object> query = map("metadata", "meta");

        testResult(db, "CALL apoc.load.html($url,$query)", map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("metadata",asList(RESULT_QUERY_METADATA)).toString().trim(), row.get("value").toString().trim());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryH2(){
        Map<String, Object> query = map("h2", "h2");

        testResult(db, "CALL apoc.load.html($url,$query)", map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
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

        testResult(db, "CALL apoc.load.html($url,$query, $config)", map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", config),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("h2",asList(RESULT_QUERY_H2)).toString().trim(), row.get("value").toString().trim());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryWithChildren() {
        Map<String, Object> query = map("toc", ".toc ul");
        Map<String, Object> config = map("children", true);

        testResult(db, "CALL apoc.load.html($url,$query, $config)", map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", config),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");

                    List<Map<String, Object>> toc = (List) value.get("toc");
                    Map<String, Object> first = toc.get(0);

                    // Should be <ul>
                    assertEquals("ul", first.get("tagName"));

                    // Should have four children
                    assertEquals(4, ((List) first.get("children")).size());

                    Map<String, Object> firstChild = (Map)((List) first.get("children")).get(0);

                    assertEquals("li", firstChild.get("tagName"));
                    assertEquals(1, ((List) firstChild.get("children")).size());
                });
    }

    @Test
    public void testQueryWithFailsSilently() {
        Map<String, Object> query = map("h2", "h2","a", "a", "invalid", "invalid");

        testResult(db, "CALL apoc.load.html($url,$query, {failSilently: true})",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    // number of <a> tags in html file minus the incorrect tag
                    assertEquals(107, ((List) value.get("a")).size());
                    assertEquals(Collections.emptyList(), value.get("invalid"));
                    assertEquals(List.of(RESULT_QUERY_H2).toString().trim(), value.get("h2").toString().trim());
                    assertFalse(result.hasNext());
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithoutFailsSilently() {
        final String url = new File("src/test/resources/wikipedia.html").toURI().toString();
        try {
            Map<String, Object> query = map("a", "a", "h2", "h2");
            testCall(db, "CALL apoc.load.html($url,$query)", map("url", url, "query", query), (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            String expectedMessage = "Error during parsing element: <a hre f=\"#cite_ref-Simpson2018_1-0\"><sup><i><b>a</b></i></sup></a>";
            assertEquals(expectedMessage, except.getMessage());
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithExceptionIfIncorrectUrl() {

        final String filePath = new File("src/test/resources/wikipedia1.html").toURI().toString();
        try {
            String query = "CALL apoc.load.html('" + filePath + "',{a:'a'})";
            testCall(db, query, (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            String expectedMessage = "File not found from: " + filePath;
            assertEquals(expectedMessage, except.getMessage());
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithExceptionIfIncorrectCharset() {
        String invalidCharset = "notValid";

        try {
            String query = "CALL apoc.load.html('" + new File("src/test/resources/wikipedia.html").toURI().toString() + "',{a:'a'}, {charset: '" + invalidCharset + "'})";
            testCall(db, query, (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            String expectedMessage = "Unsupported charset: " + invalidCharset;
            assertEquals(expectedMessage, except.getMessage());
            throw e;
        }
    }
}
