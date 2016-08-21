package apoc.export.graphml;

import apoc.export.util.BatchTransaction;
import apoc.export.util.Reporter;
import org.neo4j.graphdb.*;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mh on 10.07.13.
 */
public class XmlGraphMLReader {

    public static final String LABEL_SPLIT = " *: *";
    private final GraphDatabaseService gdb;
    private boolean storeNodeIds;
    private RelationshipType defaultRelType = RelationshipType.withName("UNKNOWN");
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

    public XmlGraphMLReader reporter(Reporter reporter) {
        this.reporter = reporter;
        return this;
    }



    enum Type {
        BOOLEAN() {
            Object parse(String value) {
                return Boolean.valueOf(value);
            }
        }, INT() {
            Object parse(String value) {
                return Integer.parseInt(value);
            }
        }, LONG() {
            Object parse(String value) {
                return Long.parseLong(value);
            }
        }, FLOAT() {
            Object parse(String value) {
                return Float.parseFloat(value);
            }
        }, DOUBLE() {
            Object parse(String value) {
                return Double.parseDouble(value);
            }

        }, STRING() {
            Object parse(String value) {
                return value;
            }
        };

        abstract Object parse(String value);

        public static Type forType(String type) {
            if (type==null) return STRING;
            return valueOf(type.trim().toUpperCase());
        }
    }

    static class Key {
        String id;
        String name;
        boolean forNode;
        Type type;
        Object defaultValue;

        public Key(String id, String name, String type, String forNode) {
            this.id = id;
            this.name = name;
            this.type = Type.forType(type);
            this.forNode = forNode == null || forNode.equalsIgnoreCase("node");
        }

        private static Key defaultKey(String id, boolean forNode) {
            return new Key(id,id,"string", forNode ? "node" : "edge");
        }

        public void setDefault(String data) {
            this.defaultValue = type.parse(data);
        }

        public Object parseValue(String input) {
            if (input == null || input.trim().isEmpty()) return defaultValue;
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
    public static final QName KEY = QName.valueOf("key");

    public XmlGraphMLReader(GraphDatabaseService gdb) {
        this.gdb = gdb;
    }

    public long parseXML(Reader input) throws XMLStreamException {
        Map<String, Long> cache = new HashMap<>(1024*32);
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        inputFactory.setProperty("javax.xml.stream.isCoalescing", true);
        XMLEventReader reader = inputFactory.createXMLEventReader(input);
        PropertyContainer last = null;
        Map<String, Key> nodeKeys = new HashMap<>();
        Map<String, Key> relKeys = new HashMap<>();
        int count = 0;
        try (BatchTransaction tx = new BatchTransaction(gdb, batchSize * 10, reporter)) {

            while (reader.hasNext()) {
                XMLEvent event = (XMLEvent) reader.next();
                if (event.isStartElement()) {

                    StartElement element = event.asStartElement();
                    String name = element.getName().getLocalPart();

                    if (name.equals("graphml") || name.equals("graph")) continue;
                    if (name.equals("key")) {
                        String id = getAttribute(element, ID);
                        Key key = new Key(id, getAttribute(element, NAME), getAttribute(element, TYPE), getAttribute(element, FOR));

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
                        Object value = key.defaultValue;
                        XMLEvent next = peek(reader);
                        if (next.isCharacters()) {
                            value = key.parseValue(reader.nextEvent().asCharacters().getData());
                        }
                        if (value != null) {
                            if (this.labels && isNode && id.equals("labels")) {
                                addLabels((Node)last,value.toString());
                            } else if (!this.labels || isNode || !id.equals("label")) {
                                last.setProperty(key.name, value);
                                if (reporter != null) reporter.update(0, 0, 1);
                            }
                        }
                        continue;
                    }
                    if (name.equals("node")) {
                        tx.increment();
                        String id = getAttribute(element, ID);
                        Node node = gdb.createNode();
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
                        String source = getAttribute(element, SOURCE);
                        String target = getAttribute(element, TARGET);
                        String label = getAttribute(element, LABEL);
                        Node from = gdb.getNodeById(cache.get(source));
                        Node to = gdb.getNodeById(cache.get(target));
                        RelationshipType type = label != null ? RelationshipType.withName(label) : defaultRelType;
                        Relationship relationship = from.createRelationshipTo(to, type);
                        setDefaults(relKeys, relationship);
                        if (reporter != null) reporter.update(0, 1, 0);
                        count++;
                        last = relationship;
                    }
                }
            }
        }
        return count;
    }

    private void addLabels(Node node, String labels) {
        if (labels==null) return;
        labels = labels.trim();
        if (labels.isEmpty()) return;
        String[] parts = labels.split(LABEL_SPLIT);
        for (String part : parts) {
            if (part.trim().isEmpty()) continue;
            node.addLabel(DynamicLabel.label(part.trim()));
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

    private void setDefaults(Map<String, Key> keys, PropertyContainer pc) {
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
