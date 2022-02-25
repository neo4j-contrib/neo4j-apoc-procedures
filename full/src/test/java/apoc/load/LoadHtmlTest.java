package apoc.load;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static apoc.load.LoadHtml.KEY_ERROR;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

    private static final String INVALID_PATH = new File("src/test/resources/wikipedia1.html").getName();
    private static final String VALID_PATH = new File("src/test/resources/wikipedia.html").toURI().toString();
    private static final String INVALID_CHARSET = "notValid";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, LoadHtml.class);
    }

    @AfterAll
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testParseGeneratedJs() {
        testCallGeneratedJsWithBrowser("FIREFOX");
        testCallGeneratedJsWithBrowser("CHROME");
    }

    @Test
    public void testWithWaitUntilAndOneElementNotFound() {
        testCall(db, "CALL apoc.load.html($url,$query,$config)",
                map("url",new File("src/test/resources/html/wikipediaWithJs.html").toURI().toString(),
                        "query", map("elementExistent", "strong", "elementNotExistent", ".asdfgh"),
                        "config", map("browser", "CHROME", "wait", 5)),
                result -> {
                    Map<String, Object> value = (Map<String, Object>) result.get("value");
                    List<Map<String, Object>> notExistent = (List<Map<String, Object>>) value.get("elementNotExistent");
                    List<Map<String, Object>> existent = (List<Map<String, Object>>) value.get("elementExistent");
                    assertTrue(notExistent.isEmpty());
                    assertEquals(1, existent.size());
                    final Map<String, Object> tag = existent.get(0);
                    assertEquals("This is a new text node", tag.get("text"));
                    assertEquals("strong", tag.get("tagName"));
                });
    }

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
                    Map<String, Object> absoluteUrlTag = map("tagName", "a", "text", "absoluteUrl",
                            "attributes", map("href", "https://foundation.wikimedia.org/wiki/Privacy_policy", "class", "urlTest"));

                    Map<String, Object> urlSameUrlTag = map("tagName", "a", "text", "urlSamePath",
                            "attributes", map("href", baseUri + "this.js", "class", "urlTest"));

                    Map<String, Object> forwardUrlTag = map("tagName", "a", "text", "forwardUrl",
                            "attributes", map("href", "file:/test.js", "class", "urlTest"));

                    Map<String, Object> backUrlTag = map("tagName", "a", "text", "backUrl",
                            "attributes", map("href", baseUri.replace("test/resources/", "backUrl.js"), "class", "urlTest"));

                    final Set<Map<String, Object>> expectedSetList = Set.of(absoluteUrlTag, urlSameUrlTag, forwardUrlTag, backUrlTag);
                    assertEquals(expectedSetList, Set.copyOf(urlTestList));
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
    public void testFailsWithIncorrectBrowser() {
        final String invalidValue = "NOT_VALID";
        final Map<String, String> config = Map.of("browser", invalidValue);
        try {
            testCall(db, "CALL apoc.load.html('" + VALID_PATH + "',{a:'a'}, $config)", Map.of("config", config), (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            String expectedMessage = "No enum constant " + LoadHtmlConfig.Browser.class.getCanonicalName() + "." + invalidValue;
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
            final String message = except.getMessage();
            assertTrue(message.startsWith("Cannot open file "));
            assertTrue(message.endsWith(INVALID_PATH + " for reading."));
            throw e;
        }
    }

    private void testCallGeneratedJsWithBrowser(String browser) {
        testCall(db, "CALL apoc.load.html($url,$query,$config)",
                map("url",new File("src/test/resources/html/wikipediaWithJs.html").toURI().toString(),
                        "query", map("td", "td", "strong", "strong"),
                        "config", map("browser", browser)),
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
