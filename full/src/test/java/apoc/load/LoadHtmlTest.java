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
import java.util.concurrent.atomic.AtomicInteger;

import static apoc.load.LoadHtml.KEY_ERROR;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


public class LoadHtmlTest {

    protected static final String RESULT_QUERY_METADATA = ("{attributes={charset=UTF-8}, tagName=meta}, " +
            "{attributes={name=ResourceLoaderDynamicStyles}, tagName=meta}, " +
            "{attributes={name=generator, content=MediaWiki 1.32.0-wmf.18}, tagName=meta}, " +
            "{attributes={name=referrer, content=origin}, tagName=meta}, " +
            "{attributes={name=referrer, content=origin-when-crossorigin}, tagName=meta}, " +
            "{attributes={name=referrer, content=origin-when-cross-origin}, tagName=meta}, " +
            "{attributes={property=og:image, content=https://upload.wikimedia.org/wikipedia/en/e/ea/Aap_Kaa_Hak_titles.jpg}, tagName=meta}");

    protected static final String RESULT_QUERY_H2 = ("{text=Contents, tagName=h2}, " +
            "{text=Origins[edit], tagName=h2}, " +
            "{text=Content[edit], tagName=h2}, " +
            "{text=Legacy[edit], tagName=h2}, " +
            "{text=References[edit], tagName=h2}, " +
            "{text=Navigation menu, tagName=h2}");

    protected static final String INVALID_PATH = new File("src/test/resources/wikipedia1.html").toURI().toString();
    protected static final String VALID_PATH = new File("src/test/resources/wikipedia.html").toURI().toString();
    protected static final String INVALID_CHARSET = "notValid";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, LoadHtml.class);
    }


    @Test
    public void testParseGeneratedJs() {
        testCallGeneratedJsWithBrowser("FIREFOX");
        testCallGeneratedJsWithBrowser("CHROME");
    }








//    @Test
//    public void TODOTESTDACANCELLARE(){
//        Map<String, Object> query = map("metadata", "meta");
//
//        testResult(db, "CALL apoc.load.html($url,$query, {withBrowser: 'FIREFOX'})", map("url", "https://kb.vmware.com/s/article/2143832", "query", query),
//                result -> {
//                    Map<String, Object> row = result.next();
//                    assertEquals(map("metadata",asList(RESULT_QUERY_METADATA)).toString().trim(), row.get("value").toString().trim());
//                    assertFalse(result.hasNext());
//                });
//    }











    @Test
    public void testWithBaseUriConfig() {
        Map<String, Object> query = map("urlTest", ".urlTest");

        final String baseUri = new File("src/test/resources").toURI().toString();
        testCall(db, "CALL apoc.load.html($url,$query, $config)",
                map("url", new File("src/test/resources/html/wikipediaWithJs.html").toURI().toString(),
                        "query", query,
                        "config", map("baseUri", baseUri)),
                result -> {
                    Map<String, Object> value = (Map<String, Object>) result.get("value");
                    final List<Map<String, Object>> urlTestList = (List<Map<String, Object>>) value.get("urlTest");
                    urlTestList.forEach(tag -> {
                        assertEquals("a", tag.get("tagName"));
                        final String href = (String) ((Map<String, Object>) tag.get("attributes")).get("href");
                        switch ((String) tag.get("text")) {
                            case "backUrl":
                                assertEquals(baseUri.replace("test/resources/", "backUrl.js"), href);
                                break;
                            case "forwardUrl":
                                assertEquals("file:/test.js", href);
                                break;
                            case "urlSamePath":
                                assertEquals(baseUri + "this.js", href);
                                break;
                            case "absoluteUrl":
                                assertEquals("https://foundation.wikimedia.org/wiki/Privacy_policy", href);
                                break;
                            default:
                                fail();
                        }
                    });
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
    public void testQueryWithFailsSilentlyWithLog() {
        Map<String, Object> query = map("a", "a", "invalid", "invalid", "h6", "h6");

        testResult(db, "CALL apoc.load.html($url,$query, {failSilently: 'WITH_LOG'})",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    // number of <a> tags in html file minus the incorrect tag
                    assertEquals(107, ((List) value.get("a")).size());
                    assertEquals(Collections.emptyList(), value.get("invalid"));
                    assertNull(value.get(KEY_ERROR));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryWithFailsSilentlyWithList() {
        Map<String, Object> query = map("a", "a", "invalid", "invalid", "h6", "h6");

        String expectedH6 = "[{attributes={id=correct}, text=test, tagName=h6}, {attributes={id=childIncorrect}, text=incorrecttest, tagName=h6}]";

        testResult(db, "CALL apoc.load.html($url,$query, {failSilently: 'WITH_LIST'})",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    // number of <a> tags in html file minus the incorrect tag
                    assertEquals(107, ((List) value.get("a")).size());
                    assertEquals(Collections.emptyList(), value.get("invalid"));
                    assertEquals(expectedH6, value.get("h6").toString().trim());
                    assertEquals(2, ((List) value.get(KEY_ERROR)).size());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryWithFailsSilentlyWithListAndChildren() {
        Map<String, Object> query = map("a", "a", "invalid", "invalid", "h6", "h6");

        String expectedH6 = "[{children=[], attributes={id=correct}, text=test, tagName=h6}, {children=[], attributes={id=childIncorrect}, text=incorrect, tagName=h6}]";

        testResult(db, "CALL apoc.load.html($url,$query, {failSilently: 'WITH_LIST', children: true})",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    // number of <a> tags in html file minus the incorrect tag
                    assertEquals(107, ((List) value.get("a")).size());
                    assertEquals(Collections.emptyList(), value.get("invalid"));
                    assertEquals(expectedH6, value.get("h6").toString().trim());
                    assertEquals(3, ((List) value.get(KEY_ERROR)).size());
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
        testIncorrectUrl("CALL apoc.load.html('" + INVALID_PATH + "',{a:'a'})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithFailsSilentlyWithLogWithExceptionIfIncorrectUrl() {
        testIncorrectUrl("CALL apoc.load.html('" + INVALID_PATH + "',{a:'a'}, {failSilently: 'WITH_LOG'})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithFailsSilentlyWithListWithExceptionIfIncorrectUrl() {
        testIncorrectUrl("CALL apoc.load.html('" + INVALID_PATH + "',{a:'a'}, {failSilently: 'WITH_LIST'})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithExceptionIfIncorrectCharset() {
        testIncorrectCharset("CALL apoc.load.html('" + VALID_PATH + "',{a:'a'}, {charset: '" + INVALID_CHARSET + "'})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithFailsSilentlyWithLogWithExceptionIfIncorrectCharset() {
        testIncorrectCharset("CALL apoc.load.html('" + VALID_PATH + "',{a:'a'}, {failSilently: 'WITH_LOG', charset: '" + INVALID_CHARSET + "'})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithFailsSilentlyWithListWithExceptionIfIncorrectCharset() {
        testIncorrectCharset("CALL apoc.load.html('" + VALID_PATH + "',{a:'a'}, {failSilently: 'WITH_LIST', charset: '" + INVALID_CHARSET + "'})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testFailsWithIncorrectCharset() {
        final Map<String, String> config = Map.of("withBrowser", "NOT_VALID");
        try {
            testCall(db, "CALL apoc.load.html('" + VALID_PATH + "',{a:'a'}, $config)", Map.of("config", config), (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            String expectedMessage = "Invalid config: " + config;
            assertEquals(expectedMessage, except.getMessage());
            throw e;
        }
    }

    private void testIncorrectCharset(String query) {
        try {
            testCall(db, query, (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            String expectedMessage = "Unsupported charset: " + INVALID_CHARSET;
            assertEquals(expectedMessage, except.getMessage());
            throw e;
        }
    }

    private void testIncorrectUrl(String query) {
        try {
            testCall(db, query, (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            String expectedMessage = "File not found from: " + INVALID_PATH;
            assertEquals(expectedMessage, except.getMessage());
            throw e;
        }
    }

    private void testCallGeneratedJsWithBrowser(String browser) {
        testCall(db, "CALL apoc.load.html($url,$query,$config)",
                map("url",new File("src/test/resources/html/wikipediaWithJs.html").toURI().toString(),
                        "query", map("td", "td", "strong", "strong"),
                        "config", map("withBrowser", browser)),
                result -> {
                    Map<String, Object> value = (Map<String, Object>) result.get("value");
                    List<Map<String, Object>> tdList = (List<Map<String, Object>>) value.get("td");
                    List<Map<String, Object>> strongList = (List<Map<String, Object>>) value.get("strong");
                    assertEquals(4, tdList.size());
                    final String templateString = "foo bar - baz";
                    AtomicInteger integer = new AtomicInteger();
                    tdList.forEach(tag -> {
                        assertEquals("td", tag.get("tagName"));
                        assertEquals(integer.getAndIncrement() + templateString, tag.get("text"));
                    });
                    assertEquals(1, strongList.size());
                    final Map<String, Object> tagStrong = strongList.get(0);
                    assertEquals("This is a new text node", tagStrong.get("text"));
                    assertEquals("strong", tagStrong.get("tagName"));
                });
    }
}
