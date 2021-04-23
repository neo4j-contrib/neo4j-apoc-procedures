package apoc.load;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static apoc.load.LoadHtmlTest.RESULT_QUERY_H2;
import static apoc.load.LoadHtmlTest.RESULT_QUERY_METADATA;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class LoadHtmlTestParameterized {
    // To check that `withBrowser` configuration preserve the result

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, LoadHtml.class);
    }


    @Parameters
    public static Collection<Object> data() {
        return List.of("notSet", "NONE", "CHROME", "FIREFOX");
    }

    @Parameter
    public String browser;


    @Test
    public void testQueryAll() {
        Map<String, Object> query = map("metadata", "meta", "h2", "h2");

        Map<String, Object> config = browserSet() ? emptyMap() : Map.of("withBrowser", browser);
        testResult(db, "CALL apoc.load.html($url,$query, $config)",
                map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", config),
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
    public void testQueryH2WithConfig() {
        Map<String, Object> query = map("h2", "h2");
        final List<String> confList = newArrayList("charset", "UTF-8", "baseUri", "");
        if (!browserSet()) {
            confList.addAll(List.of("withBrowser", browser));
        }
        Map<String, Object> config = map(confList.toArray());

        testResult(db, "CALL apoc.load.html($url,$query, $config)",
                map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", config),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("h2",asList(RESULT_QUERY_H2)).toString().trim(), row.get("value").toString().trim());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryWithChildren() {
        Map<String, Object> query = map("toc", ".toc ul");
        final List<Object> confList = newArrayList("children", true);
        if (!browserSet()) {
            confList.addAll(List.of("withBrowser", browser));
        }
        Map<String, Object> config = map(confList.toArray());

        testResult(db, "CALL apoc.load.html($url,$query, $config)",
                map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", config),
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

    private boolean browserSet() {
        return browser.equals("notSet");
    }
}
