package apoc.load;

import apoc.Extended;
import apoc.result.MapResult;
import apoc.util.Util;
import io.github.bonigarcia.wdm.WebDriverManager;
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
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Extended
public class LoadHtml {

    // public for test purpose
    public static final String KEY_ERROR = "errorList";

    private enum FailSilently { FALSE, WITH_LOG, WITH_LIST }
    private enum WithBrowser { NONE, CHROME, FIREFOX }

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;


    @Procedure
    @Description("apoc.load.html('url',{name: jquery, name2: jquery}, config) YIELD value - Load Html page and return the result as a Map")
    public Stream<MapResult> html(@Name("url") String url, @Name(value = "query",defaultValue = "{}") Map<String, String> query, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return readHtmlPage(url, query, config);
    }

    private Stream<MapResult> readHtmlPage(String url, Map<String, String> query, Map<String, Object> config) {
        String charset = config.getOrDefault("charset", "UTF-8").toString();
        try {
            // baseUri is used to resolve relative paths
            String baseUri = config.getOrDefault("baseUri", "").toString();
            WithBrowser withBrowser = WithBrowser.valueOf((String) config.getOrDefault("withBrowser", "NONE"));

            Document document;

            if (withBrowser == WithBrowser.NONE) {
                document = Jsoup.parse(Util.openInputStream(url, null, null), charset, baseUri);
            } else {
                WebDriver driver;
                if (withBrowser == WithBrowser.CHROME) {
                    WebDriverManager.chromedriver().setup();
                    ChromeOptions chromeOptions = new ChromeOptions();
                    chromeOptions.setHeadless(true);
                    chromeOptions.setAcceptInsecureCerts(true);
                    driver = new ChromeDriver(chromeOptions);
                } else {
                    WebDriverManager.firefoxdriver().setup();
                    FirefoxOptions firefoxOptions = new FirefoxOptions();
                    firefoxOptions.setHeadless(true);
                    firefoxOptions.setAcceptInsecureCerts(true);
                    driver = new FirefoxDriver(firefoxOptions);
                }
                driver.get(url);

                long wait = Util.toLong(config.getOrDefault("wait", 0));
                if (wait > 0) {
                    Wait<WebDriver> driverWait = new WebDriverWait(driver, wait);
                    try {
                        driverWait.until(webDriver -> query.values().stream()
                                .noneMatch(selector -> webDriver.findElements(By.cssSelector(selector)).isEmpty()));
                    } catch (org.openqa.selenium.TimeoutException ignored) {
                        // We continue the execution even if 1 or more elements were not found
                    }
                }
                String pageSource = driver.getPageSource();
                document = Jsoup.parse(pageSource, baseUri);

                driver.close();
            }

            Map<String, Object> output = new HashMap<>();
            List<String> errorList = new ArrayList<>();
            Document finalDocument = document;

            query.keySet().forEach(key -> {
                        Elements elements = finalDocument.select(query.get(key));
                        output.put(key, getElements(elements, config, errorList));
            });
            if (!errorList.isEmpty()) {
                output.put(KEY_ERROR, errorList);
            }

            return Stream.of(new MapResult(output));
        } catch (IllegalArgumentException | ClassCastException e) {
            throw new RuntimeException("Invalid config: " + config);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found from: " + url);
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported charset: " + charset);
        } catch(Exception e) {
            throw new RuntimeException("Can't read the HTML from: "+ url, e);
        }
    }

    private List<Map<String, Object>> getElements(Elements elements, Map<String, Object> config, List<String> errorList) {

        FailSilently failConfig = FailSilently.valueOf((String) config.getOrDefault("failSilently", "FALSE"));
        List<Map<String, Object>> elementList = new ArrayList<>();

        for (Element element : elements) {
            try {
                    Map<String, Object> result = new HashMap<>();
                    if(element.attributes().size() > 0) result.put("attributes", getAttributes(element));
                    if(!element.data().isEmpty()) result.put("data", element.data());
                    if(!element.val().isEmpty()) result.put("value", element.val());
                    if(!element.tagName().isEmpty()) result.put("tagName", element.tagName());

                    if (Util.toBoolean(config.getOrDefault("children", false))) {
                        if(element.hasText()) result.put("text", element.ownText());

                        result.put("children", getElements(element.children(), config, errorList));
                    }
                    else {
                        if(element.hasText()) result.put("text", element.text());
                    }

                    elementList.add(result);
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
        }

        return elementList;
    }

    private Map<String, String> getAttributes(Element element) {
        Map<String, String> attributes = new HashMap<>();
        for (Attribute attribute : element.attributes()) {
            if(!attribute.getValue().isEmpty()) {
                final String key = attribute.getKey();
                // with href/src attribute we prepend baseUri path
                final boolean attributeHasLink = key.equals("href") || key.equals("src");
                attributes.put(key, attributeHasLink ? element.absUrl(key) : attribute.getValue());
            }
        }

        return attributes;
    }


}