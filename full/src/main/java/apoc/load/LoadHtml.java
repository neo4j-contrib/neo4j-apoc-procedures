package apoc.load;

import apoc.Extended;
import apoc.result.MapResult;
import apoc.util.FileUtils;
import apoc.util.Util;
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

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Extended
public class LoadHtml {

    // public for test purpose
    public static final String KEY_ERROR = "errorList";

    public enum FailSilently { FALSE, WITH_LOG, WITH_LIST }

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure
    @Description("apoc.load.htmlPlainText('urlOrHtml',{name: jquery, name2: jquery}, config) YIELD value - Load Html page and return the result as a Map")
    public Stream<MapResult> htmlPlainText(@Name("urlOrHtml") String urlOrHtml, @Name(value = "query",defaultValue = "{}") Map<String, String> query, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return readHtmlPage(urlOrHtml, query, config, HtmlResultInterface.Type.PLAIN_TEXT);
    }

    @Procedure
    @Description("apoc.load.html('urlOrHtml',{name: jquery, name2: jquery}, config) YIELD value - Load Html page and return the result as a Map")
    public Stream<MapResult> html(@Name("urlOrHtml") String urlOrHtml, @Name(value = "query",defaultValue = "{}") Map<String, String> query, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return readHtmlPage(urlOrHtml, query, config, HtmlResultInterface.Type.DEFAULT);
    }

    private Stream<MapResult> readHtmlPage(String url, Map<String, String> query, Map<String, Object> config, HtmlResultInterface.Type type) {
        String charset = config.getOrDefault("charset", "UTF-8").toString();
        try {
            // baseUri is used to resolve relative paths
            String baseUri = config.getOrDefault("baseUri", "").toString();

            boolean isHtml = Util.toBoolean(config.get("htmlString"));

            Document document = isHtml 
                    ? Jsoup.parseBodyFragment(url) 
                    : Jsoup.parse(FileUtils.inputStreamFor(url), charset, baseUri);
            
            Map<String, Object> output = new HashMap<>();
            List<String> errorList = new ArrayList<>();

            query.keySet().forEach(key -> {
                final Object value = type.get().getResult(document, query.get(key), config, errorList, log);
                output.put(key, value);
            });
            if (!errorList.isEmpty()) {
                output.put(KEY_ERROR, errorList);
            }

            return Stream.of(new MapResult(output) );
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found from: " + url);
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported charset: " + charset);
        } catch(Exception e) {
            throw new RuntimeException("Can't read the HTML from: "+ url, e);
        }
    }

    public static List<Map<String, Object>> getElements(Elements elements, Map<String, Object> config, List<String> errorList, Log log) {

        FailSilently failConfig = FailSilently.valueOf((String) config.getOrDefault("failSilently", "FALSE"));
        List<Map<String, Object>> elementList = new ArrayList<>();

        for (Element element : elements) {
            withError(element, errorList, failConfig, log, () -> {
                    Map<String, Object> result = new HashMap<>();
                    if(element.attributes().size() > 0) result.put("attributes", getAttributes(element));
                    if(!element.data().isEmpty()) result.put("data", element.data());
                    if(!element.val().isEmpty()) result.put("value", element.val());
                    if(!element.tagName().isEmpty()) result.put("tagName", element.tagName());

                    if (Util.toBoolean(config.getOrDefault("children", false))) {
                        if(element.hasText()) result.put("text", element.ownText());

                        result.put("children", getElements(element.children(), config, errorList, log));
                    }
                    else {
                        if(element.hasText()) result.put("text", element.text());
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
                attributes.put(attribute.getKey(), attribute.getValue());
            }
        }

        return attributes;
    }

    public static <T> T withError(Element element, List<String> errorList, FailSilently failConfig, Log log, Supplier<T> fun) {
        
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


}