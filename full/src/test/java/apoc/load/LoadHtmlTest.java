/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.load;

import static apoc.load.LoadHtml.KEY_ERROR;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class LoadHtmlTest {

    protected static final List<Map<String, Object>> RESULT_QUERY_METADATA = asList(
            map("tagName", "meta", "attributes", map("charset", "UTF-8")),
            map("attributes", map("name", "ResourceLoaderDynamicStyles"), "tagName", "meta"),
            map("attributes", map("name", "generator", "content", "MediaWiki 1.32.0-wmf.18"), "tagName", "meta"),
            map("attributes", map("name", "referrer", "content", "origin"), "tagName", "meta"),
            map("attributes", map("name", "referrer", "content", "origin-when-crossorigin"), "tagName", "meta"),
            map("attributes", map("name", "referrer", "content", "origin-when-cross-origin"), "tagName", "meta"),
            map(
                    "attributes",
                    map(
                            "property",
                            "og:image",
                            "content",
                            "https://upload.wikimedia.org/wikipedia/en/e/ea/Aap_Kaa_Hak_titles.jpg"),
                    "tagName",
                    "meta"));

    protected static final List<Map<String, Object>> RESULT_QUERY_H2 = asList(
            map("text", "Contents", "tagName", "h2"),
            map("text", "Origins[edit]", "tagName", "h2"),
            map("text", "Content[edit]", "tagName", "h2"),
            map("text", "Legacy[edit]", "tagName", "h2"),
            map("text", "References[edit]", "tagName", "h2"),
            map("text", "Navigation menu", "tagName", "h2"));

    private static final String INVALID_PATH = new File("src/test/resources/wikipedia1.html").getName();
    private static final String VALID_PATH =
            new File("src/test/resources/wikipedia.html").toURI().toString();
    private static final String INVALID_CHARSET = "notValid";
    private static final String URL_HTML_JS =
            new File("src/test/resources/html/wikipediaWithJs.html").toURI().toString();
    private static final String CHROME = LoadHtmlConfig.Browser.CHROME.name();
    private static final String FIREFOX = LoadHtmlConfig.Browser.FIREFOX.name();

    private static final String HTML_TEXT = "<!DOCTYPE html> <html> <body> " + "<h1>My First Heading</h1> "
            + "<p class='firstClass'>My first paragraph.</p> "
            + "<p class='secondClass'>My second paragraph.</p> "
            + "<p class='thirdClass'>My third paragraph. Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book.</p> "
            + "<ul><li>Coffee</li><li>Tea</li><li>Milk</li></ul>  "
            + "</body> </html>";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule().withSetting(ApocSettings.apoc_import_file_enabled, true);

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, LoadHtml.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testParseGeneratedJs() {
        testCallGeneratedJsWithBrowser("CHROME");
    }

    @Test
    public void testParseGeneratedJsWrongConfigs() {
        String errorInvalidConfig = "Invalid config";
        assertWrongConfig(errorInvalidConfig, map("browser", CHROME, "operatingSystem", "dunno"));

        assertWrongConfig(errorInvalidConfig, map("browser", FIREFOX, "architecture", "dunno"));

        assertWrongConfig(
                "Error HTTP 401 executing", map("browser", FIREFOX, "gitHubToken", "12345", "forceDownload", true));
    }

    private void assertWrongConfig(String msgError, Map<String, Object> config) {
        try {
            testCall(
                    db,
                    "CALL apoc.load.html($url, $query, $config)",
                    map("url", URL_HTML_JS, "query", map("a", "a"), "config", config),
                    r -> fail("Should fails due to wrong configuration"));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(msgError));
        }
    }

    @Test
    public void testWithWaitUntilAndOneElementNotFound() {
        skipIfBrowserNotPresentOrCompatible(() -> {
            testCall(
                    db,
                    "CALL apoc.load.html($url,$query,$config)",
                    map(
                            "url",
                            URL_HTML_JS,
                            "query",
                            map("elementExistent", "strong", "elementNotExistent", ".asdfgh"),
                            "config",
                            map("browser", "CHROME", "wait", 5)),
                    result -> {
                        Map<String, Object> value = (Map<String, Object>) result.get("value");
                        List<Map<String, Object>> notExistent =
                                (List<Map<String, Object>>) value.get("elementNotExistent");
                        List<Map<String, Object>> existent = (List<Map<String, Object>>) value.get("elementExistent");
                        assertTrue(notExistent.isEmpty());
                        assertEquals(1, existent.size());
                        final Map<String, Object> tag = existent.get(0);
                        assertEquals("This is a new text node", tag.get("text"));
                        assertEquals("strong", tag.get("tagName"));
                    });
        });
    }

    @Test
    public void testWithBaseUriConfig() {
        Map<String, Object> query = map("urlTest", ".urlTest");

        final String baseUri = new File("src/test/resources").toURI().toString();
        testCall(
                db,
                "CALL apoc.load.html($url,$query, $config)",
                map("url", URL_HTML_JS, "query", query, "config", map("baseUri", baseUri)),
                result -> {
                    Map<String, Object> value = (Map<String, Object>) result.get("value");
                    final List<Map<String, Object>> urlTestList = (List<Map<String, Object>>) value.get("urlTest");
                    Map<String, Object> absoluteUrlTag = map(
                            "tagName",
                            "a",
                            "text",
                            "absoluteUrl",
                            "attributes",
                            map("href", "https://foundation.wikimedia.org/wiki/Privacy_policy", "class", "urlTest"));

                    Map<String, Object> urlSameUrlTag = map(
                            "tagName",
                            "a",
                            "text",
                            "urlSamePath",
                            "attributes",
                            map("href", baseUri + "this.js", "class", "urlTest"));

                    Map<String, Object> forwardUrlTag = map(
                            "tagName",
                            "a",
                            "text",
                            "forwardUrl",
                            "attributes",
                            map("href", "file:/test.js", "class", "urlTest"));

                    Map<String, Object> backUrlTag = map(
                            "tagName",
                            "a",
                            "text",
                            "backUrl",
                            "attributes",
                            map("href", baseUri.replace("test/resources/", "backUrl.js"), "class", "urlTest"));

                    final Set<Map<String, Object>> expectedSetList =
                            Set.of(absoluteUrlTag, urlSameUrlTag, forwardUrlTag, backUrlTag);
                    assertEquals(expectedSetList, Set.copyOf(urlTestList));
                });
    }

    @Test
    public void testQueryMetadata() {
        Map<String, Object> query = map("metadata", "meta");

        testResult(
                db,
                "CALL apoc.load.html($url,$query)",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("metadata", RESULT_QUERY_METADATA), row.get("value"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryMetadataWithGetElementById() {
        Map<String, Object> query = map("siteSubElement", "#siteSub");

        testCall(
                db,
                "CALL apoc.load.html($url,$query)",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                row -> {
                    final List<Map<String, Object>> expected = asList(map(
                            "attributes",
                            map("id", "siteSub", "class", "noprint"),
                            "text",
                            "From Wikipedia, the free encyclopedia",
                            "tagName",
                            "div"));
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
        final String urlFile =
                new File("src/test/resources/wikipedia.html").toURI().toString();
        testCall(
                db,
                "CALL apoc.load.html($url,$query, {failSilently: 'WITH_LOG'})",
                map("url", urlFile, "query", map("selector", selector)),
                row -> {
                    final List actual = (List) ((Map) row.get("value")).get("selector");
                    assertEquals(expected, actual.size());
                });
    }

    @Test
    public void testQueryMetadataWithGetLinks() {
        Map<String, Object> query = map("links", "a[href]");

        testCall(
                db,
                "CALL apoc.load.html($url,$query)",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                row -> {
                    final List<Map<String, Object>> actual = (List) ((Map) row.get("value")).get("links");
                    assertEquals(106, actual.size());
                    assertTrue(actual.stream().allMatch(i -> i.get("tagName").equals("a")));
                });
    }

    @Test
    public void testQueryMetadataWithGetElementsByClassAndHtmlString() {
        Map<String, Object> query = map("firstClass", ".firstClass", "secondClass", ".secondClass");

        testCall(
                db,
                "CALL apoc.load.html($html, $query, $config)",
                map("html", HTML_TEXT, "query", query, "config", map("htmlString", true)),
                row -> {
                    final Map<String, List<Map<String, Object>>> value = (Map) row.get("value");
                    final List firstClass = value.get("firstClass");
                    assertEquals(
                            map(
                                    "attributes",
                                    map("class", "firstClass"),
                                    "text",
                                    "My first paragraph.",
                                    "tagName",
                                    "p"),
                            firstClass.get(0));
                    final List secondClass = value.get("secondClass");
                    assertEquals(
                            map(
                                    "attributes",
                                    map("class", "secondClass"),
                                    "text",
                                    "My second paragraph.",
                                    "tagName",
                                    "p"),
                            secondClass.get(0));
                    System.out.println("LoadHtmlTest.testQueryMetadataWithGetElementsByClass");
                });
    }

    @Test
    public void testQueryMetadataWithGetElementsByClass() {
        Map<String, Object> query = map("siteSubElement", ".toclevel-1");

        testCall(
                db,
                "CALL apoc.load.html($url,$query)",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                row -> {
                    final List<Map<String, Object>> actual = (List) ((Map) row.get("value")).get("siteSubElement");
                    assertEquals(4, actual.size());
                    assertTrue(
                            actual.stream().allMatch(item -> item.get("tagName").equals("li")));
                });
    }

    @Test
    public void testQueryMetadataPlainText() {
        final String secondPar = "\nMy second paragraph. \n";

        final String thirdPar =
                "\nMy third paragraph. Lorem Ipsum is simply dummy text of the printing and typesetting industry. \n"
                        + "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown \n"
                        + "printer took a galley of type and scrambled it to make a type specimen book. \n";

        testCall(
                db,
                "CALL apoc.load.htmlPlainText($url, $query, {htmlString: true})",
                map("url", HTML_TEXT, "query", map("siteSubElement", "body")),
                row -> {
                    String expected = "\nMy First Heading \n" + "\nMy first paragraph. \n"
                            + secondPar
                            + thirdPar
                            + "\n"
                            + " - Coffee \n"
                            + " - Tea \n"
                            + " - Milk ";
                    final String actual = (String) ((Map) row.get("value")).get("siteSubElement");
                    assertEquals(expected, actual);
                });

        testCall(
                db,
                "CALL apoc.load.htmlPlainText($url, $query, {htmlString: true})",
                map("url", HTML_TEXT, "query", map("thirdClass", ".thirdClass", "secondClass", ".secondClass")),
                row -> {
                    final Map<String, Object> actual = (Map) row.get("value");
                    assertEquals(secondPar, actual.get("secondClass"));
                    assertEquals(thirdPar, actual.get("thirdClass"));
                });

        testCall(
                db,
                "CALL apoc.load.htmlPlainText($url,$query, {htmlString: true, textSize: 9999})",
                map("url", HTML_TEXT, "query", map("thirdClass", ".thirdClass")),
                row -> {
                    String expected =
                            "\nMy third paragraph. Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. \n";
                    assertEquals(expected, ((Map) row.get("value")).get("thirdClass"));
                });
    }

    @Test
    public void testQueryH2() {
        Map<String, Object> query = map("h2", "h2");

        testResult(
                db,
                "CALL apoc.load.html($url,$query)",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("h2", RESULT_QUERY_H2), row.get("value"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryWithFailsSilentlyWithLog() {
        Map<String, Object> query = map("a", "a", "invalid", "invalid", "h6", "h6");

        testResult(
                db,
                "CALL apoc.load.html($url,$query, {failSilently: 'WITH_LOG'})",
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
    public void testHref() {
        Map<String, Object> query = Map.of("a", "a.image");

        testResult(
                db,
                "CALL apoc.load.html($url, $query) YIELD value UNWIND value.a AS row RETURN row",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row =
                            (Map<String, Object>) result.next().get("row");
                    Map<String, Object> attributes = (Map<String, Object>) row.get("attributes");
                    Assert.assertEquals("/wiki/File:Aap_Kaa_Hak_titles.jpg", attributes.get("href"));
                });
    }

    @Test
    public void testQueryWithFailsSilentlyWithList() {
        Map<String, Object> query = map("a", "a", "invalid", "invalid", "h6", "h6");

        List<Map<String, Object>> expectedH6 = asList(
                map("attributes", map("id", "correct"), "text", "test", "tagName", "h6"),
                map("attributes", map("id", "childIncorrect"), "text", "incorrecttest", "tagName", "h6"));

        testResult(
                db,
                "CALL apoc.load.html($url,$query, {failSilently: 'WITH_LIST'})",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    // number of <a> tags in html file minus the incorrect tag
                    assertEquals(107, ((List) value.get("a")).size());
                    assertEquals(Collections.emptyList(), value.get("invalid"));
                    assertEquals(expectedH6, value.get("h6"));
                    assertEquals(2, ((List) value.get(KEY_ERROR)).size());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryWithFailsSilentlyWithListAndChildren() {
        Map<String, Object> query = map("a", "a", "invalid", "invalid", "h6", "h6");

        List<Map<String, Object>> expectedH6 = asList(
                map("children", asList(), "attributes", map("id", "correct"), "text", "test", "tagName", "h6"),
                map(
                        "children",
                        asList(),
                        "attributes",
                        map("id", "childIncorrect"),
                        "text",
                        "incorrect",
                        "tagName",
                        "h6"));

        testResult(
                db,
                "CALL apoc.load.html($url,$query, {failSilently: 'WITH_LIST', children: true})",
                map("url", new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    // number of <a> tags in html file minus the incorrect tag
                    assertEquals(107, ((List) value.get("a")).size());
                    assertEquals(Collections.emptyList(), value.get("invalid"));
                    assertEquals(expectedH6, value.get("h6"));
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
            String expectedMessage =
                    "Error during parsing element: <a hre f=\"#cite_ref-Simpson2018_1-0\"><sup><i><b>a</b></i></sup></a>";
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
        testIncorrectCharset("CALL apoc.load.html('" + VALID_PATH + "',{a:'a'}, {failSilently: 'WITH_LOG', charset: '"
                + INVALID_CHARSET + "'})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithFailsSilentlyWithListWithExceptionIfIncorrectCharset() {
        testIncorrectCharset("CALL apoc.load.html('" + VALID_PATH + "',{a:'a'}, {failSilently: 'WITH_LIST', charset: '"
                + INVALID_CHARSET + "'})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testFailsWithIncorrectBrowser() {
        final String invalidValue = "NOT_VALID";
        final Map<String, String> config = Map.of("browser", invalidValue);
        try {
            testCall(
                    db,
                    "CALL apoc.load.html('" + VALID_PATH + "',{a:'a'}, $config)",
                    Map.of("config", config),
                    (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            String expectedMessage =
                    "No enum constant " + LoadHtmlConfig.Browser.class.getCanonicalName() + "." + invalidValue;
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
        skipIfBrowserNotPresentOrCompatible(() -> {
            testCall(
                    db,
                    "CALL apoc.load.html($url,$query,$config)",
                    map(
                            "url",
                            URL_HTML_JS,
                            "query",
                            map("td", "td", "strong", "strong"),
                            "config",
                            map("browser", browser, "driverVersion", "0.30.0")),
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
        });
    }

    public static void skipIfBrowserNotPresentOrCompatible(Runnable runnable) {
        try {
            runnable.run();
        } catch (RuntimeException e) {

            // The test don't fail if the current chrome/firefox version is incompatible or if the browser is not
            // installed
            Stream<String> notPresentOrIncompatible = Stream.of(
                    "cannot find Chrome binary",
                    "Cannot find firefox binary",
                    "Expected browser binary location",
                    "browser start-up failure",
                    "This version of ChromeDriver only supports Chrome version");
            final String msg = e.getMessage();
            if (notPresentOrIncompatible.noneMatch(msg::contains)) {
                throw e;
            }
        }
    }
}
