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

import static apoc.load.LoadHtmlBrowser.getChromeInputStream;
import static apoc.load.LoadHtmlBrowser.getFirefoxInputStream;

import apoc.Extended;
import apoc.result.MapResult;
import apoc.util.FileUtils;
import apoc.util.MissingDependencyException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Extended
public class LoadHtml {

    // public for test purpose
    public static final String KEY_ERROR = "errorList";
    public static final String SELENIUM_MISSING_DEPS_ERROR = "Cannot find the Selenium client jar.\n"
            + "Please put the apoc-selenium-dependencies-5.x.x-all.jar into plugin folder.\n"
            + "See the documentation: https://neo4j.com/labs/apoc/5/overview/apoc.load/apoc.load.html/#selenium-dependencies";

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure
    @Description(
            "apoc.load.htmlPlainText('urlOrHtml',{name: jquery, name2: jquery}, config) YIELD value - Load Html page and return the result as a Map")
    public Stream<MapResult> htmlPlainText(
            @Name("urlOrHtml") String urlOrHtml,
            @Name(value = "query", defaultValue = "{}") Map<String, String> query,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return readHtmlPage(urlOrHtml, query, config, HtmlResultInterface.Type.PLAIN_TEXT);
    }

    @Procedure
    @Description(
            "apoc.load.html('url',{name: jquery, name2: jquery}, config) YIELD value - Load Html page and return the result as a Map")
    public Stream<MapResult> html(
            @Name("url") String url,
            @Name(value = "query", defaultValue = "{}") Map<String, String> query,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return readHtmlPage(url, query, config, HtmlResultInterface.Type.DEFAULT);
    }

    private Stream<MapResult> readHtmlPage(
            String url, Map<String, String> query, Map<String, Object> conf, HtmlResultInterface.Type type) {
        LoadHtmlConfig config = new LoadHtmlConfig(conf);
        try {
            // baseUri is used to resolve relative paths
            Document document = config.isHtmlString()
                    ? Jsoup.parseBodyFragment(url)
                    : Jsoup.parse(getHtmlInputStream(url, query, config), config.getCharset(), config.getBaseUri());

            Map<String, Object> output = new HashMap<>();
            List<String> errorList = new ArrayList<>();

            query.keySet().forEach(key -> {
                final Object value = type.get().getResult(document, query.get(key), config, errorList, log);
                output.put(key, value);
            });
            if (!errorList.isEmpty()) {
                output.put(KEY_ERROR, errorList);
            }

            return Stream.of(new MapResult(output));
        } catch (UnsupportedCharsetException e) {
            throw new RuntimeException("Unsupported charset: " + config.getCharset());
        } catch (IllegalArgumentException | ClassCastException e) {
            throw new RuntimeException("Invalid config: " + e.getMessage());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found from: " + url);
        } catch (Exception e) {
            throw new RuntimeException("Can't read the HTML from: " + url, e);
        }
    }

    private InputStream getHtmlInputStream(String url, Map<String, String> query, LoadHtmlConfig config)
            throws IOException {

        final boolean isHeadless = config.isHeadless();
        final boolean isAcceptInsecureCerts = config.isAcceptInsecureCerts();
        switch (config.getBrowser()) {
            case FIREFOX:
                return withSeleniumBrowser(
                        () -> getFirefoxInputStream(url, query, config, isHeadless, isAcceptInsecureCerts));
            case CHROME:
                return withSeleniumBrowser(
                        () -> getChromeInputStream(url, query, config, isHeadless, isAcceptInsecureCerts));
            default:
                return FileUtils.inputStreamFor(url, null, null, null);
        }
    }

    public static List<Map<String, Object>> getElements(
            Elements elements, LoadHtmlConfig conf, List<String> errorList, Log log) {

        List<Map<String, Object>> elementList = new ArrayList<>();

        for (Element element : elements) {
            withError(element, errorList, conf.getFailSilently(), log, () -> {
                Map<String, Object> result = new HashMap<>();
                if (element.attributes().size() > 0) result.put("attributes", getAttributes(element));
                if (!element.data().isEmpty()) result.put("data", element.data());
                if (!element.val().isEmpty()) result.put("value", element.val());
                if (!element.tagName().isEmpty()) result.put("tagName", element.tagName());

                if (conf.isChildren()) {
                    if (element.hasText()) result.put("text", element.ownText());

                    result.put("children", getElements(element.children(), conf, errorList, log));
                } else {
                    if (element.hasText()) result.put("text", element.text());
                }
                elementList.add(result);
                return null;
            });
        }

        return elementList;
    }

    private static Map<String, String> getAttributes(Element element) {
        Map<String, String> attributes = new HashMap<>();
        for (Attribute attribute : element.attributes()) {
            if (!attribute.hasDeclaredValue() && !Attribute.isBooleanAttribute(attribute.getKey())) {
                throw new RuntimeException("Invalid tag " + element);
            }
            if (!attribute.getValue().isBlank()) {
                final String key = attribute.getKey();
                // with href/src attribute we prepend baseUri path
                final boolean attributeHasLink = key.equals("href") || key.equals("src");
                String attr = null;
                if (attributeHasLink) {
                    attr = element.absUrl(key);
                    if (StringUtils.isBlank(attr)) {
                        attr = attribute.getValue();
                    }
                } else {
                    attr = attribute.getValue();
                }
                attributes.put(key, attr);
            }
        }

        return attributes;
    }

    public static <T> T withError(
            Element element, List<String> errorList, LoadHtmlConfig.FailSilently failConfig, Log log, Supplier<T> fun) {

        try {
            return fun.get();
        } catch (Exception e) {
            final String parseError = "Error during parsing element: " + element;
            switch (failConfig) {
                case WITH_LOG:
                    log.warn(parseError);
                    break;
                case WITH_LIST:
                    errorList.add(element.toString());
                    break;
                default:
                    throw new RuntimeException(parseError);
            }
        }
        return null;
    }

    private InputStream withSeleniumBrowser(Supplier<InputStream> action) {
        try {
            return action.get();
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(SELENIUM_MISSING_DEPS_ERROR);
        }
    }
}
