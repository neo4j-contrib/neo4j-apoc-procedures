package apoc.load;

import apoc.result.MapResult;
import apoc.util.FileUtils;
import apoc.util.MapUtil;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LoadHtml {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;


    @Procedure
    @Description("apoc.load.html('url',{name: jquery, name2: jquery}, config) YIELD value - Load Html page and return the result as a Map")
    public Stream<MapResult> html(@Name("url") String url, @Name(value = "query",defaultValue = "{}") Map<String, String> query, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return readHtmlPage(url, query, config);
    }

    private Stream<MapResult> readHtmlPage(String url, Map<String, String> query, Map<String, Object> config){
        String charset = config.getOrDefault("charset", "UTF-8").toString();
        try {
            // baseUri is used to resolve relative paths
            String baseUri = config.getOrDefault("baseUri", "").toString();

            Document document = Jsoup.parse(FileUtils.inputStreamFor(url, null, null), charset, baseUri);

            return query.keySet().stream().map(key -> {
                Elements elements = document.select(query.get(key));
                List<Map<String, Object>> resultList = new ArrayList<>();
                getElements(elements, resultList);

                return new MapResult(MapUtil.map(key, resultList));
            });
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found from: " + url);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported charset: " + charset);
        } catch (Exception e) {
            throw new RuntimeException("Can't read the HTML from: "+ url, e);
        }
    }

    private void getElements(Elements elements, List<Map<String, Object>> resultList) {
        for (Element element : elements) {
            Map<String, Object> result = new HashMap<>();
            if(element.attributes().size() > 0) result.put("attributes", getAttributes(element));
            if(!element.data().isEmpty())result.put("data", element.data());
            if(element.hasText()) result.put("text", element.text());
            if(!element.val().isEmpty()) result.put("value", element.val());
            if(!element.tagName().isEmpty()) result.put("tagName", element.tagName());

            resultList.add(result);
        }
    }

    private Map<String, String> getAttributes(Element element) {
        Map<String, String> attributes = new HashMap<>();
        for (Attribute attribute : element.attributes()) {
            if(!attribute.getValue().isEmpty()) attributes.put(attribute.getKey(), attribute.getValue());
        }

        return attributes;
    }


}