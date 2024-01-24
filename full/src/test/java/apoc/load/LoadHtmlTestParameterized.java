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

import static apoc.load.LoadHtmlTest.RESULT_QUERY_H2;
import static apoc.load.LoadHtmlTest.RESULT_QUERY_METADATA;
import static apoc.load.LoadHtmlTest.skipIfBrowserNotPresentOrCompatible;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

// TODO: Reintroduce FIREFOX as a browser https://trello.com/c/X8KM7sFU/1803-fix-flaky-selenium-firefox-tests
@RunWith(Parameterized.class)
public class LoadHtmlTestParameterized {
    // Tests taken from LoadHtmlTest.java.
    // To check that `browser` configuration preserve the result.

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

    @Parameters
    public static Collection<Object> data() {
        return List.of("notSet", "NONE", "CHROME");
    }

    @Parameter
    public String browser;

    @Test
    public void testQueryAll() {
        Map<String, Object> query = map("metadata", "meta", "h2", "h2");

        Map<String, Object> config = browserSet() ? Map.of("browser", browser) : emptyMap();

        skipIfBrowserNotPresentOrCompatible(() -> {
            testResult(
                    db,
                    "CALL apoc.load.html($url,$query, $config)",
                    map(
                            "url",
                            new File("src/test/resources/wikipedia.html")
                                    .toURI()
                                    .toString(),
                            "query",
                            query,
                            "config",
                            config),
                    result -> {
                        Map<String, Object> row = result.next();
                        Map<String, Object> value = (Map<String, Object>) row.get("value");

                        List<Map<String, Object>> metadata = (List<Map<String, Object>>) value.get("metadata");
                        List<Map<String, Object>> h2 = (List<Map<String, Object>>) value.get("h2");
    
                        assertEquals(RESULT_QUERY_METADATA, metadata);
                        assertEquals(RESULT_QUERY_H2, h2);
                    });
        });
    }

    @Test
    public void testQueryH2WithConfig() {
        Map<String, Object> query = map("h2", "h2");
        final List<Object> confList = newArrayList("charset", "UTF-8", "baseUri", "");
        addBrowserIfSet(confList);
        Map<String, Object> config = map(confList.toArray());

        skipIfBrowserNotPresentOrCompatible(() -> {
            testResult(
                    db,
                    "CALL apoc.load.html($url, $query, $config)",
                    map(
                            "url",
                            new File("src/test/resources/wikipedia.html")
                                    .toURI()
                                    .toString(),
                            "query",
                            query,
                            "config",
                            config),
                    result -> {
                        Map<String, Object> row = result.next();
                        assertEquals(map("h2",RESULT_QUERY_H2), row.get("value"));
                        assertFalse(result.hasNext());
                    });
        });
    }

    @Test
    public void testQueryWithChildren() {
        Map<String, Object> query = map("toc", ".toc ul");
        final List<Object> confList = newArrayList("children", true);
        addBrowserIfSet(confList);
        Map<String, Object> config = map(confList.toArray());

        skipIfBrowserNotPresentOrCompatible(() -> {
            testResult(
                    db,
                    "CALL apoc.load.html($url, $query, $config)",
                    map(
                            "url",
                            new File("src/test/resources/wikipedia.html")
                                    .toURI()
                                    .toString(),
                            "query",
                            query,
                            "config",
                            config),
                    result -> {
                        Map<String, Object> row = result.next();
                        Map<String, Object> value = (Map<String, Object>) row.get("value");

                        List<Map<String, Object>> toc = (List) value.get("toc");
                        Map<String, Object> first = toc.get(0);

                        // Should be <ul>
                        assertEquals("ul", first.get("tagName"));

                        // Should have four children
                        assertEquals(4, ((List) first.get("children")).size());

                        Map<String, Object> firstChild = (Map) ((List) first.get("children")).get(0);

                        assertEquals("li", firstChild.get("tagName"));
                        assertEquals(1, ((List) firstChild.get("children")).size());
                    });
        });
    }

    private void addBrowserIfSet(List<Object> confList) {
        if (browserSet()) {
            confList.addAll(List.of("browser", browser));
        }
    }

    private boolean browserSet() {
        return !browser.equals("notSet");
    }
}
