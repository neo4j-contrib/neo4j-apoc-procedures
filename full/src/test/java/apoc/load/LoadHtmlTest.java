package apoc.load;

import apoc.ApocSettings;
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

    private static final String INVALID_PATH = new File("src/test/resources/wikipedia1.html").getName();
    private static final String INVALID_PATH_ABSOLUTE = new File("src/test/resources/wikipedia1.html").getName();
    private static final String VALID_PATH = new File("src/test/resources/wikipedia.html").toURI().toString();
    private static final String INVALID_CHARSET = "notValid";
    
    private static final String HTML_TEXT = "<!DOCTYPE html> <html> <body> " +
            "<h1>My First Heading</h1> " +
            "<p class='firstClass'>My first paragraph.</p> " +
            "<p class='secondClass'>My second paragraph.</p> " +
            "<p class='thirdClass'>My third paragraph. Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book.</p> " +
            "<ul><li>Coffee</li><li>Tea</li><li>Milk</li></ul>  " +
            "</body> </html>";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

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
    public void testQueryMetadataWithGetElementById() {
        Map<String, Object> query = map("siteSubElement", "#siteSub");

        testCall(db, "CALL apoc.load.html($url,$query)", 
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                row -> {
                    final List<Map<String, Object>> expected = asList(map("attributes", map("id", "siteSub", "class", "noprint"),
                            "text", "From Wikipedia, the free encyclopedia",
                            "tagName", "div"));
                    final Object actual = ((Map<String, Object>) row.get("value")).get("siteSubElement");
                    assertEquals(expected, actual);
                });
    }
    
    @Test
    public void shouldEmulateJsoupFunctions() {
        // getElementsByTag
        loadHtmlWithSelector(48, "div");
        
        // getElementsByAttribute
        loadHtmlWithSelector(10, "[type]");
        
        // getElementsByAttributeStarting
        loadHtmlWithSelector(15, "[^ro]");
        
        // getElementsByAttributeValue
        loadHtmlWithSelector(1, "*[role=button]");
        
        // getElementsByAttributeValueContaining
        loadHtmlWithSelector(1, "[role*=utto]");
        
        // getElementsByAttributeValueEnding
        loadHtmlWithSelector(4, "*[class$=editsection]");
        
        // getElementsByAttributeValueNot
        loadHtmlWithSelector(426, "*:not([class=\"mw-editsection\"])");
        
        // getElementsByAttributeValueStarting
        loadHtmlWithSelector(2, "div[id^=site]");
        
        // getElementsByAttributeValueMatching
        loadHtmlWithSelector(3, "div[id~=content]");
        
        // getElementsByIndexEquals
        loadHtmlWithSelector(3, "*:nth-child(12)");
        
        // getElementsByIndexGreaterThan
        loadHtmlWithSelector(12, "*:gt(12)");
        
        // getElementsByIndexLessThan
        loadHtmlWithSelector(47, "div:lt(12)");
        
        // getElementsContainingOwnText
        loadHtmlWithSelector(5, "i:containsOwn(Kaa)");
        
        // getElementsContainingText
        loadHtmlWithSelector(6, "i:contains(Kaa)");
        
        // getElementsContainingText
        loadHtmlWithSelector(6, "i:matches((?i)kaa)");
        
        // getElementsContainingText
        loadHtmlWithSelector(5, "i:matchesOwn((?i)kaa)");
        
        // getAllElements
        loadHtmlWithSelector(430, "*");
    }

    private void loadHtmlWithSelector(int expected, String selector) {
        final String urlFile = new File("src/test/resources/wikipedia.html").toURI().toString();
        testCall(db, "CALL apoc.load.html($url,$query, {failSilently: 'WITH_LOG'})",
                map("url", urlFile, "query", map("selector", selector)),
                row -> {
                    final List actual = (List) ((Map) row.get("value")).get("selector");
                    assertEquals(expected, actual.size());
                });
    }

    @Test
    public void testQueryMetadataWithGetLinks() {
        Map<String, Object> query = map("links", "a[href]");

        testCall(db, "CALL apoc.load.html($url,$query)", 
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                row -> {
                    final List<Map<String, Object>> actual = (List) ((Map) row.get("value")).get("links");
                    assertEquals(106, actual.size());
                    assertTrue(actual.stream().allMatch(i -> i.get("tagName").equals("a")));
                });
    }

    @Test
    public void testQueryMetadataWithGetElementsByClassAndHtmlString() {
        Map<String, Object> query = map("firstClass", ".firstClass",
                "secondClass", ".secondClass");

        testCall(db, "CALL apoc.load.html($html, $query, $config)", 
                map("html", HTML_TEXT, 
                        "query", query,
                        "config", map("htmlString", true)),
                row -> {
                    final Map<String, List<Map<String, Object>>> value = (Map) row.get("value");
                    final List firstClass = value.get("firstClass");
                    assertEquals(map("attributes", map("class", "firstClass"), "text", "My first paragraph.", "tagName", "p"), firstClass.get(0));
                    final List secondClass = value.get("secondClass");
                    assertEquals(map("attributes", map("class", "secondClass"), "text", "My second paragraph.", "tagName", "p"), secondClass.get(0));
                    System.out.println("LoadHtmlTest.testQueryMetadataWithGetElementsByClass");
                });
    }

    @Test
    public void testQueryMetadataWithGetElementsByClass() {
        Map<String, Object> query = map("siteSubElement", ".toclevel-1");

        testCall(db, "CALL apoc.load.html($url,$query)", 
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                row -> {
                    final List<Map<String, Object>> actual = (List) ((Map) row.get("value")).get("siteSubElement");
                    assertEquals(4, actual.size());
                    assertTrue(actual.stream().allMatch(item -> item.get("tagName").equals("li")));
                });
    }

    @Test
    public void testQueryMetadataPlainText() {
        final String secondPar = "\nMy second paragraph. \n";
        
        final String thirdPar = "\nMy third paragraph. Lorem Ipsum is simply dummy text of the printing and typesetting industry. \n" +
                "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown \n" +
                "printer took a galley of type and scrambled it to make a type specimen book. \n";

        testCall(db, "CALL apoc.load.htmlPlainText($url, $query, {htmlString: true})", 
                map("url", HTML_TEXT, "query", map("siteSubElement", "body")),
                row -> {
                    String expected = "\nMy First Heading \n" +
                            "\nMy first paragraph. \n" +
                            secondPar +
                            thirdPar +
                            "\n" +
                            " - Coffee \n" +
                            " - Tea \n" +
                            " - Milk ";
                    final String actual = (String) ((Map) row.get("value")).get("siteSubElement");
                    assertEquals(expected, actual);
                });

        testCall(db, "CALL apoc.load.htmlPlainText($url, $query, {htmlString: true})",
                map("url", HTML_TEXT, "query", 
                        map("thirdClass", ".thirdClass", "secondClass", ".secondClass")),
                row -> {
                    final Map<String, Object> actual = (Map) row.get("value");
                    assertEquals(secondPar, actual.get("secondClass"));
                    assertEquals(thirdPar, actual.get("thirdClass"));
                });
        
        testCall(db, "CALL apoc.load.htmlPlainText($url,$query, {htmlString: true, textSize: 9999})",
                map("url", HTML_TEXT, "query", map("thirdClass", ".thirdClass")),
                row -> {
                    String expected = "\nMy third paragraph. Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. \n";
                    assertEquals(expected, ((Map) row.get("value")).get("thirdClass"));
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
        testIncorrectUrl("CALL apoc.load.html('" + INVALID_PATH + "',{failSilently: 'WITH_LOG', a:'a'})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithFailsSilentlyWithListWithExceptionIfIncorrectUrl() {
        testIncorrectUrl("CALL apoc.load.html('" + INVALID_PATH + "',{failSilently: 'WITH_LIST', a:'a'})");
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
}
