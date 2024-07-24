package apoc.load.util;

import apoc.export.util.BatchTransaction;
import apoc.export.util.CountingInputStream;
import apoc.export.util.ExportConfig;
import apoc.export.util.Reporter;
import apoc.result.MapResult;
import apoc.util.CompressionAlgo;
import apoc.util.FileUtils;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.TerminationGuard;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static apoc.load.util.XmlReadUtil.Load.generateXmlDoctypeException;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.ExtendedUtil.toValidValue;

/**
 * Taken from <a href="https://github.com/neo4j/apoc/blob/dev/core/src/main/java/apoc/load/Xml.java">Xml</a>
 * placed in APOC Core
 */
public class XmlReadUtil {

    public static class Load {
        public static Stream<MapResult> xmlXpathToMapResult(
                Object urlOrBinary, URLAccessChecker urlAccessChecker, TerminationGuard terminationGuard, Map<String, Object> config) throws Exception {
            if (config == null) config = Collections.emptyMap();
            boolean failOnError = (boolean) config.getOrDefault("failOnError", true);
            String path = (String) config.getOrDefault("path", "/");
            boolean simpleMode = Util.toBoolean(config.getOrDefault("simpleMode", false));
            try {
                Map<String, Object> headers = (Map) config.getOrDefault("headers", Collections.emptyMap());
                CountingInputStream is = FileUtils.inputStreamFor(
                        urlOrBinary,
                        headers,
                        null,
                        (String) config.getOrDefault(COMPRESSION, CompressionAlgo.NONE.name()),
                        urlAccessChecker);
                return parse(is, simpleMode, path, failOnError, terminationGuard);
            } catch (Exception e) {
                if (!failOnError) return Stream.of(new MapResult(Collections.emptyMap()));
                else throw e;
            }
        }

        private static Stream<MapResult> parse(InputStream data, boolean simpleMode, String path, boolean failOnError, TerminationGuard terminationGuard)
                throws Exception {
            List<MapResult> result = new ArrayList<>();
            try {
                DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
                documentBuilderFactory.setNamespaceAware(true);
                documentBuilderFactory.setIgnoringElementContentWhitespace(true);
                documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
                DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
                documentBuilder.setEntityResolver((publicId, systemId) -> new InputSource(new StringReader("")));

                Document doc = documentBuilder.parse(data);
                XPathFactory xPathFactory = XPathFactory.newInstance();

                XPath xPath = xPathFactory.newXPath();

                path = StringUtils.isEmpty(path) ? "/" : path;
                XPathExpression xPathExpression = xPath.compile(path);
                NodeList nodeList = (NodeList) xPathExpression.evaluate(doc, XPathConstants.NODESET);

                for (int i = 0; i < nodeList.getLength(); i++) {
                    final Deque<Map<String, Object>> stack = new LinkedList<>();
                    handleNode(stack, nodeList.item(i), simpleMode, terminationGuard);
                    for (int index = 0; index < stack.size(); index++) {
                        result.add(new MapResult(stack.pollFirst()));
                    }
                }
            } catch (FileNotFoundException e) {
                if (!failOnError) return Stream.of(new MapResult(Collections.emptyMap()));
                else throw e;
            } catch (Exception e) {
                if (!failOnError) return Stream.of(new MapResult(Collections.emptyMap()));
                else if (e instanceof SAXParseException && e.getMessage().contains("DOCTYPE is disallowed"))
                    throw generateXmlDoctypeException();
                else throw e;
            }
            return result.stream();
        }

        /**
         * Collects type and attributes for the node
         *
         * @param node
         * @param elementMap
         */
        private static void handleTypeAndAttributes(org.w3c.dom.Node node, Map<String, Object> elementMap) {
            // Set type
            if (node.getLocalName() != null) {
                elementMap.put("_type", node.getLocalName());
            }

            // Set the attributes
            if (node.getAttributes() != null) {
                NamedNodeMap attributeMap = node.getAttributes();
                for (int i = 0; i < attributeMap.getLength(); i++) {
                    org.w3c.dom.Node attribute = attributeMap.item(i);
                    elementMap.put(attribute.getNodeName(), attribute.getNodeValue());
                }
            }
        }

        private static void handleNode(Deque<Map<String, Object>> stack, org.w3c.dom.Node node, boolean simpleMode, TerminationGuard terminationGuard) {
            terminationGuard.check();

            // Handle document node
            if (node.getNodeType() == org.w3c.dom.Node.DOCUMENT_NODE) {
                NodeList children = node.getChildNodes();
                for (int i = 0; i < children.getLength(); i++) {
                    if (children.item(i).getLocalName() != null) {
                        handleNode(stack, children.item(i), simpleMode, terminationGuard);
                        return;
                    }
                }
            }

            Map<String, Object> elementMap = new LinkedHashMap<>();
            handleTypeAndAttributes(node, elementMap);

            // Set children
            NodeList children = node.getChildNodes();
            int count = 0;
            for (int i = 0; i < children.getLength(); i++) {
                org.w3c.dom.Node child = children.item(i);

                // This is to deal with text between xml tags for example new line characters
                if (child.getNodeType() != org.w3c.dom.Node.TEXT_NODE && child.getNodeType() != org.w3c.dom.Node.CDATA_SECTION_NODE) {
                    handleNode(stack, child, simpleMode, terminationGuard);
                    count++;
                } else {
                    // Deal with text nodes
                    handleTextNode(child, elementMap);
                }
            }

            if (children.getLength() > 0) {
                if (!stack.isEmpty()) {
                    List<Object> nodeChildren = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        nodeChildren.add(stack.pollLast());
                    }
                    String key = simpleMode ? "_" + node.getLocalName() : "_children";
                    Collections.reverse(nodeChildren);
                    if (nodeChildren.size() > 0) {
                        // Before adding the children we need to handle mixed text
                        Object text = elementMap.get("_text");
                        if (text instanceof List) {
                            for (Object element : (List) text) {
                                nodeChildren.add(element);
                            }
                            elementMap.remove("_text");
                        }

                        elementMap.put(key, nodeChildren);
                    }
                }
            }

            if (!elementMap.isEmpty()) {
                stack.addLast(elementMap);
            }
        }

        /**
         * Handle TEXT nodes and CDATA nodes
         *
         * @param node
         * @param elementMap
         */
        private static void handleTextNode(org.w3c.dom.Node node, Map<String, Object> elementMap) {
            Object text = "";
            int nodeType = node.getNodeType();
            switch (nodeType) {
                case org.w3c.dom.Node.TEXT_NODE:
                    text = normalizeText(node.getNodeValue());
                    break;
                case org.w3c.dom.Node.CDATA_SECTION_NODE:
                    text = normalizeText(((CharacterData) node).getData());
                    break;
                default:
                    break;
            }

            // If the text is valid ...
            if (!StringUtils.isEmpty(text.toString())) {
                // We check if we have already collected some text previously
                Object previousText = elementMap.get("_text");
                if (previousText != null) {
                    // If we just have a "_text" key than we need to collect to a List
                    text = Arrays.asList(previousText.toString(), text);
                }
                elementMap.put("_text", text);
            }
        }

        /**
         * Remove trailing whitespaces and new line characters
         *
         * @param text
         * @return
         */
        private static String normalizeText(String text) {
            String[] tokens = StringUtils.split(text, "\n");
            for (int i = 0; i < tokens.length; i++) {
                tokens[i] = tokens[i].trim();
            }

            return StringUtils.join(tokens, " ").trim();
        }

        public static RuntimeException generateXmlDoctypeException() {
            throw new RuntimeException("XML documents with a DOCTYPE are not allowed.");
        }
    }


    /**
     * Taken from <a href="https://github.com/neo4j/apoc/blob/dev/core/src/main/java/apoc/export/graphml/GraphMLReader.java">GraphMLReader</a>
     * placed in APOC Core
     */
    public static class Import {

        public static final String LABEL_SPLIT = " *: *";
        private final GraphDatabaseService db;
        private boolean storeNodeIds;
        private RelationshipType defaultRelType = RelationshipType.withName("UNKNOWN");
        private ExportConfig.NodeConfig source;
        private ExportConfig.NodeConfig target;
        private int batchSize = 40000;
        private Reporter reporter;
        private boolean labels;

        public Import storeNodeIds() {
            this.storeNodeIds = true;
            return this;
        }

        public Import relType(String name) {
            this.defaultRelType = RelationshipType.withName(name);
            return this;
        }

        public Import batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Import nodeLabels(boolean readLabels) {
            this.labels = readLabels;
            return this;
        }

        public Import source(ExportConfig.NodeConfig sourceConfig) {
            this.source = sourceConfig;
            return this;
        }

        public Import target(ExportConfig.NodeConfig targetConfig) {
            this.target = targetConfig;
            return this;
        }

        public Import reporter(Reporter reporter) {
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
                    return Type.parseList(value, Boolean.class, (i) -> (Boolean) i);
                }
            },
            INT() {
                Object parse(String value) {
                    return Integer.parseInt(value);
                }

                Object parseList(String value) {
                    return Type.parseList(value, Integer.class, (n) -> ((Number) n).intValue());
                }
            },
            LONG() {
                Object parse(String value) {
                    return Long.parseLong(value);
                }

                Object parseList(String value) {
                    return Type.parseList(value, Long.class, (i) -> ((Number) i).longValue());
                }
            },
            FLOAT() {
                Object parse(String value) {
                    return Float.parseFloat(value);
                }

                Object parseList(String value) {
                    return Type.parseList(value, Float.class, (i) -> ((Number) i).floatValue());
                }
            },
            DOUBLE() {
                Object parse(String value) {
                    return Double.parseDouble(value);
                }

                Object parseList(String value) {
                    return Type.parseList(value, Double.class, (i) -> ((Number) i).doubleValue());
                }
            },
            STRING() {
                Object parse(String value) {
                    return value;
                }

                Object parseList(String value) {
                    return Type.parseList(value, String.class, (i) -> (String) i);
                }
            };

            abstract Object parse(String value);

            abstract Object parseList(String value);

            public static <T> T[] parseList(String value, Class<T> asClass, Function<Object, T> convert) {
                List parsed = JsonUtil.parse(value, null, List.class);
                T[] converted = (T[]) Array.newInstance(asClass, parsed.size());

                for (int i = 0; i < parsed.size(); i++) converted[i] = convert.apply(parsed.get(i));
                return converted;
            }

            public static Type forType(String type) {
                if (type == null) return STRING;
                return valueOf(type.trim().toUpperCase());
            }
        }

        static class Key {
            String nameOrId;
            boolean forNode;
            Type listType;
            Type type;
            Object defaultValue;

            public Key(String nameOrId, String type, String listType, String forNode) {
                this.nameOrId = nameOrId;
                this.type = Type.forType(type);
                if (listType != null) {
                    this.listType = Type.forType(listType);
                }
                this.forNode = forNode == null || forNode.equalsIgnoreCase("node");
            }

            private static Key defaultKey(String id, boolean forNode) {
                return new Key(id, "string", null, forNode ? "node" : "edge");
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
        public static final QName LABEL = QName.valueOf("label");
        public static final QName VALUE = QName.valueOf("value");
        public static final QName FOR = QName.valueOf("for");
        public static final QName NAME = QName.valueOf("attr.name");
        public static final QName TYPE = QName.valueOf("attr.type");
        public static final QName DATA_TYPE = QName.valueOf("type");
        public static final QName LIST = QName.valueOf("attr.list");
        public static final QName KEY = QName.valueOf("key");
        public static final QName KIND = QName.valueOf("kind");

        public Import(GraphDatabaseService db) {
            this.db = db;
        }

        public long parseXML(Reader input, TerminationGuard terminationGuard) throws XMLStreamException {
            Map<String, Object> dataMap = new HashMap<>();
            Map<String, String> cache = new HashMap<>(1024 * 32);
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            inputFactory.setProperty("javax.xml.stream.isCoalescing", true);
            inputFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
            inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
            XMLEventReader reader = inputFactory.createXMLEventReader(input);
            Entity last = null;
            Map<String, Key> nodeKeys = new HashMap<>();
            Map<String, Key> relKeys = new HashMap<>();
            int count = 0;
            BatchTransaction tx = new BatchTransaction(db, batchSize * 10, reporter);
            try {

                while (reader.hasNext()) {
                    terminationGuard.check();
                    XMLEvent event;
                    try {
                        event = (XMLEvent) reader.next();
                        if (event.getEventType() == XMLStreamConstants.DTD) {
                            generateXmlDoctypeException();
                        }
                    } catch (Exception e) {
                        // in case of unicode invalid chars we skip the event, or we exit in case of EOF
                        if (e.getMessage().contains("Unexpected EOF")) {
                            break;
                        } else if (e.getMessage().contains("DOCTYPE")) {
                            throw e;
                        }
                        continue;
                    }
                    if (event.isStartElement()) {

                        StartElement element = event.asStartElement();
                        String name = element.getName().getLocalPart();

                        if (name.equals("graphml") || name.equals("graph") || name.equals("gexf")) continue;
                        if (name.equals("attribute")) {
                            String id = getAttribute(element, ID);
                            String type = getAttribute(element, DATA_TYPE);
                            dataMap.put(id, type);
                        }
                        if (name.equals("key")) {
                            String id = getAttribute(element, ID);
                            Key key = new Key(
                                    getAttribute(element, NAME),
                                    getAttribute(element, TYPE),
                                    getAttribute(element, LIST),
                                    getAttribute(element, FOR));

                            XMLEvent next = peek(reader);
                            if (next.isStartElement()
                                    && next.asStartElement()
                                    .getName()
                                    .getLocalPart()
                                    .equals("default")) {
                                reader.nextEvent().asStartElement();
                                key.setDefault(reader.nextEvent().asCharacters().getData());
                            }
                            if (key.forNode) nodeKeys.put(id, key);
                            else relKeys.put(id, key);
                            continue;
                        }
                        if (name.equals("attvalue")) { // Changed from data to attvalue for node properties in gexf
                            if (last == null) continue;
                            String id = getAttribute(element, FOR);
                            boolean isNode = last instanceof Node;
                            Key key = isNode ? nodeKeys.get(id) : relKeys.get(id);
                            if (key == null) key = Key.defaultKey(id, isNode);
                            final Map.Entry<XMLEvent, Object> eventEntry = getDataEventEntry(reader, key);
                            final XMLEvent next = eventEntry.getKey();
                            final Object value = getAttribute(element, VALUE);
                            if (value != null) {
                                if (this.labels && isNode && id.equals("labels")) {
                                    addLabels((Node) last, value.toString());
                                } else if (!this.labels || isNode || !id.equals("label")) {
                                    Object convertedValue = toValidValue(value, key.nameOrId, dataMap);
                                    last.setProperty(key.nameOrId, convertedValue);
                                    if (reporter != null) reporter.update(0, 0, 1);
                                }
                            } else if (next.getEventType() == XMLStreamConstants.END_ELEMENT) {
                                last.setProperty(key.nameOrId, StringUtils.EMPTY);
                                reporter.update(0, 0, 1);
                            }
                            continue;
                        }
                        if (name.equals("node")) {
                            tx.increment();
                            String id = getAttribute(element, ID);
                            Node node = tx.getTransaction().createNode();
                            if (this.labels) {
                                String labels = getAttribute(element, LABEL); // Changed from labels to label to fit gexf property format
                                addLabels(node, labels);
                            }
                            if (storeNodeIds) node.setProperty("id", id);
                            setDefaults(nodeKeys, node);
                            last = node;
                            cache.put(id, node.getElementId());
                            if (reporter != null) reporter.update(1, 0, 0);
                            count++;
                            continue;
                        }
                        if (name.equals("edge")) {
                            tx.increment();
                            String label = getAttribute(element, KIND); // changed from label to kind for gexf
                            Node from = getByNodeId(cache, tx.getTransaction(), element, NodeExport.NodeType.SOURCE);
                            Node to = getByNodeId(cache, tx.getTransaction(), element, NodeExport.NodeType.TARGET);

                            RelationshipType relationshipType =
                                    label == null ? getRelationshipType(reader) : RelationshipType.withName(label);
                            Relationship relationship = from.createRelationshipTo(to, relationshipType);
                            setDefaults(relKeys, relationship);
                            last = relationship;
                            if (reporter != null) reporter.update(0, 1, 0);
                            count++;
                        }
                    }
                }
                tx.doCommit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            } finally {
                tx.close();
                reader.close();
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
                    data = StringUtils.join(data, reader.nextEvent().asCharacters().getData());
                    return peekRecursively(reader, data);
                }
                // in case the event is not a char we continue setting labels/properties
                return new AbstractMap.SimpleEntry<>(peek, data);
            } catch (Exception e) {
                // in case of unicode invalid chars we continue until we get a valid event
                return peekRecursively(reader, data);
            }
        }

        private Node getByNodeId(
                Map<String, String> cache, Transaction tx, StartElement element, NodeExport.NodeType nodeType) {
            final NodeExport xmlNodeInterface = nodeType.get();
            final ExportConfig.NodeConfig nodeConfig = xmlNodeInterface.getNodeConfigReader(this);

            final String sourceTargetValue = getAttribute(element, QName.valueOf(nodeType.getName()));

            final String id = cache.get(sourceTargetValue);
            // without source/target config, we look for the internal id
            if (StringUtils.isBlank(nodeConfig.label)) {
                return tx.getNodeByElementId(id);
            }
            // with source/target configured, we search a node with a specified label
            // and with a type specified in sourceType, if present, or string by default
            final String attribute = getAttribute(element, QName.valueOf(nodeType.getNameType()));
            final Object value =
                    attribute == null ? sourceTargetValue : Type.forType(attribute).parse(sourceTargetValue);

            return tx.findNode(
                    Label.label(nodeConfig.label),
                    Optional.ofNullable(nodeConfig.id).orElse("id"),
                    value);
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

                boolean notStartElementOrContainsKeyLabel = isChar || !peek.isStartElement() || containsLabelKey(peek);

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
            if (labels == null) return;
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
                if (key.defaultValue != null) pc.setProperty(key.nameOrId, key.defaultValue);
            }
        }

        private String getAttribute(StartElement element, QName qname) {
            Attribute attribute = element.getAttributeByName(qname);
            return attribute != null ? attribute.getValue() : null;
        }
    }

    /**
     * Taken from <a href="https://github.com/neo4j/apoc/blob/dev/core/src/main/java/apoc/export/graphml/NodeExport.java">NodeExport</a>
     * placed in APOC Core
     */
    interface NodeExport {

        ExportConfig.NodeConfig getNodeConfigReader(Import reader);

        enum NodeType {
            SOURCE("source", Import::getSource),

            TARGET("target", Import::getTarget);

            private final String name;
            private final NodeExport exportNode;

            NodeType(String name, NodeExport exportNode) {
                this.name = name;
                this.exportNode = exportNode;
            }

            public String getName() {
                return name;
            }

            public String getNameType() {
                return name + "Type";
            }

            NodeExport get() {
                return exportNode;
            }
        }
    }
}
