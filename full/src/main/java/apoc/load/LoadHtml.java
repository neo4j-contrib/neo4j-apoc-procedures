package apoc.load;

import apoc.Extended;
import apoc.export.util.CountingInputStream;
import apoc.result.MapResult;
import apoc.util.Util;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
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
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.edge.EdgeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Stream;


@Extended
public class LoadHtml {

    // public for test purpose
    public static final String KEY_ERROR = "errorList";

    private enum FailSilently { FALSE, WITH_LOG, WITH_LIST }

    private enum WithGeneratedJs { NONE, CHROME, FIREFOX }

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;


    @Procedure
    @Description("apoc.load.html('url',{name: jquery, name2: jquery}, config) YIELD value - Load Html page and return the result as a Map")
    public Stream<MapResult> html(@Name("url") String url, @Name(value = "query",defaultValue = "{}") Map<String, String> query, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        System.out.println("PORCO DIO!");
        return readHtmlPage(url, query, config);
    }

    private Stream<MapResult> readHtmlPage(String url, Map<String, String> query, Map<String, Object> config) {
        String charset = config.getOrDefault("charset", "UTF-8").toString();
        try {
            // baseUri is used to resolve relative paths
            String baseUri = config.getOrDefault("baseUri", "").toString();
            WithGeneratedJs withGeneratedJs = WithGeneratedJs.valueOf((String) config.getOrDefault("withGeneratedJs", "NONE"));

            Document document;

            final CountingInputStream in = Util.openInputStream(url, null, null);
            if (withGeneratedJs == WithGeneratedJs.NONE) {
                document = Jsoup.parse(in, charset, baseUri);
            } else {
                WebDriver driver;

                // TODO - TESTARE CON BASE URI

                final byte[] bytes = in.readAllBytes();
                String stringona = new String(bytes);
                System.out.println(stringona);

                if (withGeneratedJs == WithGeneratedJs.CHROME) {
                    WebDriverManager.chromedriver().setup();
                    ChromeOptions options = new ChromeOptions();
                    options.setHeadless(true);
                    driver = new ChromeDriver(options);
                } else {
                    WebDriverManager.firefoxdriver().setup();
                    FirefoxOptions options = new FirefoxOptions();
                    options.setHeadless(true);
                    driver = new FirefoxDriver(options);
                }

                driver.get(url);
                final String pageSource = driver.getPageSource();
                document = Jsoup.parse(pageSource, baseUri);
                driver.close();
            }
//            if (withGeneratedJs) {
//                try {

//                    System.out.println("LoadHtml.readHtmlPage");

//                    try(final WebClient webClient = new WebClient()) {
//                        webClient.setJavaScriptTimeout();

//                        WebDriverManager.chromedriver().setup();


//DesiredCapabilities caps = new DesiredCapabilities();
//caps.setJavascriptEnabled(true);
//caps.

//                        ChromeOptions options = new ChromeOptions();
//                        options.setHeadless(true);
//                        options.addArguments("--headless", "--disable-gpu", "--window-size=1920,1200","--ignore-certificate-errors");

//                        WebDriver ghostDriver = new ChromeDriver(options);
//                        ghostDriver.get(url);

//                        final String pageSource = ghostDriver.getPageSource();

//                        final HtmlPage page1 = webClient.getPage(new URL(url));
//
//                        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
//                        webClient.getOptions().setThrowExceptionOnScriptError(false);
//                        webClient.getOptions().setJavaScriptEnabled(true);
//                        webClient.getOptions().setCssEnabled(true);
//                        webClient.getOptions().setUseInsecureSSL(true);
//                        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
//                        webClient.getCookieManager().setCookiesEnabled(true);
//            webClient.waitForBackgroundJavaScript(15000);
//                webClient.setAjaxController(new NicelyResynchronizingAjaxController());
////
////
//                final HtmlPage page = webClient.getPage(url);
//
//                // TODO TODOISSIMO --> COSI FUNZIONA ((HtmlPage) webClient.getPage(url)).asXml()
//
//                        final WebRequest request = new WebRequest(new URL(url));
//                        request.setCharset(Charset.forName(charset));
//                        final HtmlPage page = webClient.getPage(request);
//                        webClient.waitForBackgroundJavaScript(10 * 1000);
//                        final String s = page.asXml();
//                        System.out.println(s);
////            final Document parse = Jsoup.parse(s, baseUri);
////            parse.select("td");
//
////
//////            synchronized (page) {
//////                try {
//////                    page.wait(15000);
//////                } catch (InterruptedException e) {
//////                    e.printStackTrace();
//////                }
//////            }
////
//                System.out.println(page);
//
//
//                        document = Jsoup.parse(pageSource, baseUri);

                        // todo - autoclosable?

//                    }
//                    document = Jsoup.parse(Util.openInputStream(url, null, null), charset, baseUri);
//                } catch (Exception e) {
//                    System.out.println("ERRORONE!!!!!!");
//                    throw new RuntimeException(e);
//                }
//            } else {



//                document = Jsoup.parse(Util.openInputStream(url, null, null), charset, baseUri);
//            }

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
            throw new RuntimeException("Invalid config", e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found from: " + url);
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported charset: " + charset);
        } catch(Exception e) {
            throw new RuntimeException("Can't read the HTML from: "+ url, e);
        }
    }


//    private static Document renderPage(String filePath) {
//        System.setProperty("phantomjs.binary.path", "libs/phantomjs"); // path to bin file. NOTE: platform dependent
//        WebDriver ghostDriver = new PhantomJSDriver();
//        try {
//            ghostDriver.get(filePath);
//            return Jsoup.parse(ghostDriver.getPageSource());
//        } finally {
//            ghostDriver.quit();
//        }
//    }

    /*
    webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);

    IterableUtils.size(((Iterable)
    ( (DomElement)    ((List) ((HtmlPage) webClient.getPage(
                new URL(url)
        //        "file:/" + Path.of(url).toString().replace("file:", "")
        )).getElementsByTagName("h2")).get(0)
    ).getChildElements()))


     */

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
            if(!attribute.getValue().isEmpty()) attributes.put(attribute.getKey(), attribute.getValue());
        }

        return attributes;
    }


}