package apoc.export.graphml;

import apoc.export.util.BatchTransaction;
import apoc.export.util.ExportConfig;
import apoc.export.util.Reporter;
import apoc.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.Reader;
import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Created by mh on 10.07.13.
 */
public class XmlGraphMLReader {

    public static final String LABEL_SPLIT = " *: *";
    private final GraphDatabaseService db;
    private final Transaction tx;
    private boolean storeNodeIds;
    private RelationshipType defaultRelType = RelationshipType.withName("UNKNOWN");
    private ExportConfig.NodeConfig source;
    private ExportConfig.NodeConfig target;
    private int batchSize = 40000;
    private Reporter reporter;
    private boolean labels;

    public XmlGraphMLReader storeNodeIds() {
        this.storeNodeIds = true;
        return this;
    }

    public XmlGraphMLReader relType(String name) {
        this.defaultRelType = RelationshipType.withName(name);
        return this;
    }

    public XmlGraphMLReader batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public XmlGraphMLReader nodeLabels(boolean readLabels) {
        this.labels = readLabels;
        return this;
    }

    public XmlGraphMLReader source(ExportConfig.NodeConfig sourceConfig) {
        this.source = sourceConfig;
        return this;
    }

    public XmlGraphMLReader target(ExportConfig.NodeConfig targetConfig) {
        this.target = targetConfig;
        return this;
    }

    public XmlGraphMLReader reporter(Reporter reporter) {
        this.reporter = reporter;
        return this;
    }

    public ExportConfig.NodeConfig getSource() {
        return source;
    }

    public ExportConfig.NodeConfig getTarget() {
        return target;
    }

    enum Type {
        BOOLEAN() {
            Object parse(String value) {
                return Boolean.valueOf(value);
            }

            Object parseList(String value) {
                return Type.parseList(value, Boolean.class, (i) -> (Boolean)i);
            }
        }, INT() {
            Object parse(String value) {
                return Integer.parseInt(value);
            }

            Object parseList(String value) {
                return Type.parseList(value, Integer.class, (n) -> ((Number)n).intValue());
            }
        }, LONG() {
            Object parse(String value) {
                return Long.parseLong(value);
            }

            Object parseList(String value) {
                return Type.parseList(value, Long.class, (i) -> ((Number)i).longValue());
            }
        }, FLOAT() {
            Object parse(String value) {
                return Float.parseFloat(value);
            }

            Object parseList(String value) {
                return Type.parseList(value, Float.class, (i) -> ((Number)i).floatValue());
            }
        }, DOUBLE() {
            Object parse(String value) {
                return Double.parseDouble(value);
            }

            Object parseList(String value) {
                return Type.parseList(value, Double.class, (i) -> ((Number)i).doubleValue());
            }
        }, STRING() {
            Object parse(String value) {
                return value;
            }

            Object parseList(String value) {
                return Type.parseList(value, String.class, (i) -> (String)i);
            }
        };

        abstract Object parse(String value);
        abstract Object parseList(String value);

        public static <T> T[] parseList(String value, Class<T> asClass, Function<Object, T> convert) {
            List parsed = JsonUtil.parse(value, null, List.class);
            T[] converted = (T[])Array.newInstance(asClass, parsed.size());

            for (int i = 0; i < parsed.size(); i++)
                converted[i] = convert.apply(parsed.get(i));
            return converted;
        }

        public static Type forType(String type) {
            if (type==null) return STRING;
            return valueOf(type.trim().toUpperCase());
        }
    }

    static class Key {
        String id;
        String name;
        boolean forNode;
        Type listType;
        Type type;
        Object defaultValue;

        public Key(String id, String name, String type, String listType, String forNode) {
            this.id = id;
            this.name = name;
            this.type = Type.forType(type);
            if (listType != null) {
                this.listType = Type.forType(listType);
            }
            this.forNode = forNode == null || forNode.equalsIgnoreCase("node");
        }

        private static Key defaultKey(String id, boolean forNode) {
            return new Key(id,id,"string", null, forNode ? "node" : "edge");
        }

        public void setDefault(String data) {
            this.defaultValue = type.parse(data);
        }

        public Object parseValue(String input) {
            if (input == null || input.trim().isEmpty()) return defaultValue;
            if (listType != null) return listType.parseList(input);
            return type.parse(input);
        }
    }

    public static final QName ID = QName.valueOf("id");
    public static final QName LABELS = QName.valueOf("labels");
    public static final QName SOURCE = QName.valueOf("source");
    public static final QName TARGET = QName.valueOf("target");
    public static final QName LABEL = QName.valueOf("label");
    public static final QName FOR = QName.valueOf("for");
    public static final QName NAME = QName.valueOf("attr.name");
    public static final QName TYPE = QName.valueOf("attr.type");
    public static final QName LIST = QName.valueOf("attr.list");
    public static final QName KEY = QName.valueOf("key");

    public XmlGraphMLReader(GraphDatabaseService db, Transaction tx) {
        this.db = db;
        this.tx = tx;
    }

    public long parseXML(Reader input) throws XMLStreamException {
        Map<String, Long> cache = new HashMap<>(1024*32);
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        inputFactory.setProperty("javax.xml.stream.isCoalescing", true);
        inputFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
        XMLEventReader reader = inputFactory.createXMLEventReader(input);
        Entity last = null;
        Map<String, Key> nodeKeys = new HashMap<>();
        Map<String, Key> relKeys = new HashMap<>();
        int count = 0;
        try (BatchTransaction tx = new BatchTransaction(db, batchSize * 10, reporter)) {

            while (reader.hasNext()) {
                XMLEvent event;
                try {
                    event = (XMLEvent) reader.next();
                } catch (Exception e) {
                    // in case of unicode invalid chars we skip the event, or we exit in case of EOF
                    if (e.getMessage().contains("Unexpected EOF")) {
                        break;
                    }
                    continue;
                }
                if (event.isStartElement()) {

                    StartElement element = event.asStartElement();
                    String name = element.getName().getLocalPart();

                    if (name.equals("graphml") || name.equals("graph")) continue;
                    if (name.equals("key")) {
                        String id = getAttribute(element, ID);
                        Key key = new Key(id, getAttribute(element, NAME), getAttribute(element, TYPE), getAttribute(element, LIST), getAttribute(element, FOR));

                        XMLEvent next = peek(reader);
                        if (next.isStartElement() && next.asStartElement().getName().getLocalPart().equals("default")) {
                            reader.nextEvent().asStartElement();
                            key.setDefault(reader.nextEvent().asCharacters().getData());
                        }
                        if (key.forNode) nodeKeys.put(id, key);
                        else relKeys.put(id, key);
                        continue;
                    }
                    if (name.equals("data")) {
                        if (last == null) continue;
                        String id = getAttribute(element, KEY);
                        boolean isNode = last instanceof Node;
                        Key key = isNode ? nodeKeys.get(id) : relKeys.get(id);
                        if (key == null) key = Key.defaultKey(id, isNode);
                        final Map.Entry<XMLEvent, Object> eventEntry = getDataEventEntry(reader, key);
                        final XMLEvent next = eventEntry.getKey();
                        final Object value = eventEntry.getValue();
                        if (value != null) {
                            if (this.labels && isNode && id.equals("labels")) {
                                addLabels((Node)last,value.toString());
                            } else if (!this.labels || isNode || !id.equals("label")) {
                                last.setProperty(key.name, value);
                                if (reporter != null) reporter.update(0, 0, 1);
                            }
                        } else if (next.getEventType() == XMLStreamConstants.END_ELEMENT) {
                            last.setProperty(key.name, StringUtils.EMPTY);
                            reporter.update(0, 0, 1);
                        }
                        continue;
                    }
                    if (name.equals("node")) {
                        tx.increment();
                        String id = getAttribute(element, ID);
                        Node node = tx.getTransaction().createNode();
                        if (this.labels) {
                            String labels = getAttribute(element, LABELS);
                            addLabels(node, labels);
                        }
                        if (storeNodeIds) node.setProperty("id", id);
                        setDefaults(nodeKeys, node);
                        last = node;
                        cache.put(id, node.getId());
                        if (reporter != null) reporter.update(1, 0, 0);
                        count++;
                        continue;
                    }
                    if (name.equals("edge")) {
                        tx.increment();
                        String label = getAttribute(element, LABEL);
                        Node from = getByNodeId(cache, tx.getTransaction(), element, XmlNodeExport.NodeType.SOURCE);
                        Node to = getByNodeId(cache, tx.getTransaction(), element, XmlNodeExport.NodeType.TARGET);

                        RelationshipType relationshipType = label == null ? getRelationshipType(reader) : RelationshipType.withName(label);
                        Relationship relationship = from.createRelationshipTo(to, relationshipType);
                        setDefaults(relKeys, relationship);
                        last = relationship;
                        if (reporter != null) reporter.update(0, 1, 0);
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private Map.Entry<XMLEvent, Object> getDataEventEntry(XMLEventReader reader, Key key) {
        Object value = key.defaultValue;
        
        final Map.Entry<XMLEvent, String> peekEntry = peekRecursively(reader, null);
        if (peekEntry.getValue() != null) {
            value = key.parseValue(peekEntry.getValue());
        }
        return new AbstractMap.SimpleEntry<>(peekEntry.getKey(), value);
    }

    private Map.Entry<XMLEvent, String> peekRecursively(XMLEventReader reader, String data) {
        try {
            final XMLEvent peek = peek(reader);
            // in case of char, we concat the result to the current value and redo the peek
            //  in order to obtain e.g. from a string "ab<invalid_char>cd<invalid_char>ef" --> "abcdef"
            if (peek.isCharacters()) {
                data = StringUtils.join(data,
                        reader.nextEvent().asCharacters().getData());
                return peekRecursively(reader, data);
            }
            // in case the event is not a char we continue setting labels/properties 
            return new AbstractMap.SimpleEntry<>(peek, data);
        } catch (Exception e) {
            // in case of unicode invalid chars we continue until we get a valid event
            return peekRecursively(reader, data);
        }
    }

    private Node getByNodeId(Map<String, Long> cache, Transaction tx, StartElement element, XmlNodeExport.NodeType nodeType) {
        final XmlNodeExport.ExportNode xmlNodeInterface = nodeType.get();
        final ExportConfig.NodeConfig nodeConfig = xmlNodeInterface.getNodeConfigReader(this);
        
        final String sourceTargetValue = getAttribute(element, QName.valueOf(nodeType.getName()));
        
        final Long id = cache.get(sourceTargetValue);
        // without source/target config, we look for the internal id
        if (StringUtils.isBlank(nodeConfig.label)) {
            return tx.getNodeById(id);
        }
        // with source/target configured, we search a node with a specified label 
        // and with a type specified in sourceType, if present, or string by default
        final String attribute = getAttribute(element, QName.valueOf(nodeType.getNameType()));
        final Object value = attribute == null 
                ? sourceTargetValue 
                : Type.forType(attribute).parse(sourceTargetValue);
        
        return tx.findNode(Label.label(nodeConfig.label), Optional.ofNullable(nodeConfig.id).orElse("id"), value);
    }

    private RelationshipType getRelationshipType(XMLEventReader reader) throws XMLStreamException {
        if (this.labels) {
            XMLEvent peek = reader.peek();
            boolean isChar = peek.isCharacters();
            if (isChar && !(peek.asCharacters().isWhiteSpace())) {
                String value = peek.asCharacters().getData();
                String el = ":";
                String typeRel = value.contains(el) ? value.replace(el, StringUtils.EMPTY) : value;
                return RelationshipType.withName(typeRel.trim());
            }

            boolean notStartElementOrContainsKeyLabel = isChar
                    || !peek.isStartElement()
                    || containsLabelKey(peek);

            if (!peek.isEndDocument() && notStartElementOrContainsKeyLabel) {
                reader.nextEvent();
                return getRelationshipType(reader);
            }
        }
        reader.nextEvent(); // to prevent eventual wrong reader (f.e. self-closing tag)
        return defaultRelType;
    }

    private boolean containsLabelKey(XMLEvent peek) {
        final Attribute keyAttribute = peek.asStartElement().getAttributeByName(new QName("key"));
        return keyAttribute != null && keyAttribute.getValue().equals("label");
    }

    private void addLabels(Node node, String labels) {
        if (labels==null) return;
        labels = labels.trim();
        if (labels.isEmpty()) return;
        String[] parts = labels.split(LABEL_SPLIT);
        for (String part : parts) {
            if (part.trim().isEmpty()) continue;
            node.addLabel(Label.label(part.trim()));
        }
    }

    private XMLEvent peek(XMLEventReader reader) throws XMLStreamException {
        XMLEvent peek = reader.peek();
        if (peek.isCharacters() && (peek.asCharacters().isWhiteSpace())) {
            reader.nextEvent();
            return peek(reader);
        }
        return peek;
    }

    private void setDefaults(Map<String, Key> keys, Entity pc) {
        if (keys.isEmpty()) return;
        for (Key key : keys.values()) {
            if (key.defaultValue!=null) pc.setProperty(key.name,key.defaultValue);
        }
    }

    private String getAttribute(StartElement element, QName qname) {
        Attribute attribute = element.getAttributeByName(qname);
        return attribute != null ? attribute.getValue() : null;
    }
}
