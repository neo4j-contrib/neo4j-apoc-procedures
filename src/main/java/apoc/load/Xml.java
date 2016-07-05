package apoc.load;

import apoc.Description;
import apoc.result.MapResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Xml {

    public static final XMLInputFactory FACTORY = XMLInputFactory.newFactory();

    @Context public GraphDatabaseService db;

    @Procedure
    @Description("apoc.load.xml('http://example.com/test.xml') YIELD value as doc CREATE (p:Person) SET p.name = doc.name load from XML URL (e.g. web-api) to import XML as single nested map with attributes and _type, _text and _childrenx fields.")
    public Stream<MapResult> xml(@Name("url") String url) {
        try {
            URLConnection urlConnection = new URL(url).openConnection();
            FACTORY.setProperty("javax.xml.stream.isCoalescing", true);
            XMLStreamReader reader = FACTORY.createXMLStreamReader(urlConnection.getInputStream());
            if (reader.nextTag()==XMLStreamConstants.START_ELEMENT) {
                return Stream.of(new MapResult(handleElement(reader)));
            }
            throw new RuntimeException("Can't read url " + url + " as XML");
        } catch (IOException | XMLStreamException e) {
            throw new RuntimeException("Can't read url " + url + " as XML", e);
        }
    }
    @Procedure
    @Description("apoc.load.xml('http://example.com/test.xml') YIELD value as doc CREATE (p:Person) SET p.name = doc.name load from XML URL (e.g. web-api) to import XML as single nested map with attributes and _type, _text and _childrenx fields.")
    public Stream<MapResult> xmlSimple(@Name("url") String url) {
        try {
            URLConnection urlConnection = new URL(url).openConnection();
            FACTORY.setProperty("javax.xml.stream.isCoalescing", true);
            XMLStreamReader reader = FACTORY.createXMLStreamReader(urlConnection.getInputStream());
            if (reader.nextTag()==XMLStreamConstants.START_ELEMENT) {
                return Stream.of(new MapResult(handleElementSimple(null,reader)));
            }
            throw new RuntimeException("Can't read url " + url + " as XML");
        } catch (IOException | XMLStreamException e) {
            throw new RuntimeException("Can't read url " + url + " as XML", e);
        }
    }

    private Map<String, Object> handleElement(XMLStreamReader reader) throws XMLStreamException {
        LinkedHashMap<String, Object> row = null;
        String element = null;
        if (reader.isStartElement()) {
            int attributes = reader.getAttributeCount();
            row = new LinkedHashMap<>(attributes + 3);
            element = reader.getLocalName();
            row.put("_type", element);
            for (int a = 0; a < attributes; a++) {
                row.put(reader.getAttributeLocalName(a), reader.getAttributeValue(a));
            }
            next(reader);
            if (reader.hasText()) {
                row.put("_text",reader.getText().trim());
                next(reader);
            }
            if (reader.isStartElement()) {
                List<Map<String, Object>> children = new ArrayList<>(100);
                do {
                    Map<String, Object> child = handleElement(reader);
                    if (child != null && !child.isEmpty()) {
                        children.add(child);
                    }
                } while (next(reader) == XMLStreamConstants.START_ELEMENT);
                if (!children.isEmpty()) row.put("_children", children);
            }
            if (reader.isEndElement() || reader.getEventType() == XMLStreamConstants.END_DOCUMENT) {
                return row;
            }
        }
        throw new IllegalStateException("Incorrect end-element state "+reader.getEventType()+" after "+element);
    }
    private Map<String, Object> handleElementSimple(Map<String,Object> parent, XMLStreamReader reader) throws XMLStreamException {
        LinkedHashMap<String, Object> row = null;
        String element = null;
        if (reader.isStartElement()) {
            int attributes = reader.getAttributeCount();
            row = new LinkedHashMap<>(attributes + 3);
            element = reader.getLocalName();
            row.put("_type", element);
            for (int a = 0; a < attributes; a++) {
                row.put(reader.getAttributeLocalName(a), reader.getAttributeValue(a));
            }
            if (parent!=null) {
                Object children = parent.get("_"+element);
                if (children == null) parent.put("_"+element, row);
                else if (children instanceof List) ((List)children).add(row);
                else {
                    List list = new ArrayList<>();
                    list.add(children);
                    list.add(row);
                    parent.put("_"+element, list);
                }
            }
            next(reader);
            if (reader.hasText()) {
                row.put("_text",reader.getText().trim());
                next(reader);
            }
            if (reader.isStartElement()) {
                do {
                    handleElementSimple(row, reader);
                } while (next(reader) == XMLStreamConstants.START_ELEMENT);
            }
            if (reader.isEndElement() || reader.getEventType() == XMLStreamConstants.END_DOCUMENT) {
                return row;
            }
        }
        throw new IllegalStateException("Incorrect end-element state "+reader.getEventType()+" after "+element);
    }

    private int next(XMLStreamReader reader) throws XMLStreamException {
        reader.next();
        while (reader.isWhiteSpace()) reader.next();
        return reader.getEventType();
    }
}
