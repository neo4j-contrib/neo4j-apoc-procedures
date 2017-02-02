package apoc.load;

import org.neo4j.procedure.Description;
import apoc.export.util.FileUtils;
import apoc.result.MapResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.stream.Stream;

import static apoc.util.Util.cleanUrl;
import static javax.xml.stream.XMLStreamConstants.*;

public class Xml {

    public static final XMLInputFactory FACTORY = XMLInputFactory.newFactory();

    @Context public GraphDatabaseService db;

    @Procedure
    @Description("apoc.load.xml('http://example.com/test.xml', false) YIELD value as doc CREATE (p:Person) SET p.name = doc.name load from XML URL (e.g. web-api) to import XML as single nested map with attributes and _type, _text and _childrenx fields.")
    public Stream<MapResult> xml(@Name("url") String url, @Name(value = "simple", defaultValue = "false") boolean simpleMode) {
        return xmlToMapResult(url, simpleMode);
    }

    @Procedure(deprecatedBy = "apoc.load.xml")
    @Deprecated
    @Description("apoc.load.xmlSimple('http://example.com/test.xml') YIELD value as doc CREATE (p:Person) SET p.name = doc.name load from XML URL (e.g. web-api) to import XML as single nested map with attributes and _type, _text and _children fields. This method does intentionally not work with XML mixed content.")
    public Stream<MapResult> xmlSimple(@Name("url") String url) {
        return xmlToMapResult(url, true);
    }

    private Stream<MapResult> xmlToMapResult(@Name("url") String url, boolean simpleMode) {
        try {
            XMLStreamReader reader = getXMLStreamReaderFromUrl(url);
            final Deque<Map<String, Object>> stack = new LinkedList<>();
            do {
                handleXmlEvent(stack, reader, simpleMode);
            } while (proceedReader(reader));
            return Stream.of(new MapResult(stack.getFirst()));
        } catch (IOException | XMLStreamException e) {
            throw new RuntimeException("Can't read url " + cleanUrl(url) + " as XML", e);
        }
    }

    private XMLStreamReader getXMLStreamReaderFromUrl(@Name("url") String url) throws IOException, XMLStreamException {
        FileUtils.checkReadAllowed(url);
        URLConnection urlConnection = new URL(url).openConnection();
        FACTORY.setProperty("javax.xml.stream.isCoalescing", true);
        return FACTORY.createXMLStreamReader(urlConnection.getInputStream());
    }


    private boolean proceedReader(XMLStreamReader reader) throws XMLStreamException {
        if (reader.hasNext()) {
            do {
                reader.next();
            } while (reader.isWhiteSpace());
            return true;
        } else {
            return false;
        }
    }

    private void handleXmlEvent(Deque<Map<String, Object>> stack, XMLStreamReader reader, boolean simpleMode) throws XMLStreamException {
        Map<String, Object> elementMap;
        switch (reader.getEventType()) {
            case START_DOCUMENT:
            case END_DOCUMENT:
                // intentionally empty
                break;
            case START_ELEMENT:
                int attributes = reader.getAttributeCount();
                elementMap = new LinkedHashMap<>(attributes+3);
                elementMap.put("_type", reader.getLocalName());
                for (int a = 0; a < attributes; a++) {
                    elementMap.put(reader.getAttributeLocalName(a), reader.getAttributeValue(a));
                }
                if (!stack.isEmpty()) {
                    final Map<String, Object> last = stack.getLast();
                    String key = simpleMode ? "_" + reader.getLocalName() : "_children";
                    amendToList(last, key, elementMap);
                }

                stack.addLast(elementMap);
                break;

            case END_ELEMENT:
                elementMap = stack.size() > 1 ? stack.removeLast() : stack.getLast();

                // maintain compatibility with previous implementation:
                // if we only have text childs, return them in "_text" and not in "_children"
                Object children = elementMap.get("_children");
                if (children!= null) {
                    if ((children instanceof String) || collectionIsAllStrings(children) ) {
                        elementMap.put("_text", children);
                        elementMap.remove("_children");
                    }
                }
                break;

            case CHARACTERS:
                final String text = reader.getText().trim();
                if (!text.isEmpty()) {
                    Map<String, Object> map = stack.getLast();
                    amendToList(map, "_children", text);
                }
                break;

            default:
                throw new RuntimeException("dunno know how to handle xml event type " + reader.getEventType());
        }
    }

    private boolean collectionIsAllStrings(Object collection) {
        if (collection instanceof Collection) {
            return ((Collection<Object>)collection).stream().allMatch(o -> o instanceof String);
        } else {
            return false;
        }
    }

    private void amendToList(Map<String, Object> map, String key, Object value) {
        final Object element = map.get(key);
        if (element == null ) {
            map.put(key, value);
        } else {
            if (element instanceof List) {
                ((List)element).add(value);
            } else {
                List<Object> list = new LinkedList<>();
                list.add(element);
                list.add(value);
                map.put(key, list);
            }
        }
    }
}
