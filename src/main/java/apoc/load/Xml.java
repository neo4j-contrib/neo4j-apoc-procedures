package apoc.load;

import apoc.export.util.FileUtils;
import apoc.result.MapResult;
import apoc.result.NodeResult;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.*;
import org.w3c.dom.CharacterData;
import org.w3c.dom.*;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.stream.Stream;

import static apoc.util.Util.cleanUrl;
import static javax.xml.stream.XMLStreamConstants.*;

public class Xml {

    public static final XMLInputFactory FACTORY = XMLInputFactory.newFactory();

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.load.xml('http://example.com/test.xml', 'xPath', false) YIELD value as doc CREATE (p:Person) SET p.name = doc.name load from XML URL (e.g. web-api) to import XML as single nested map with attributes and _type, _text and _childrenx fields.")
    public Stream<MapResult> xml(@Name("url") String url, @Name(value = "path", defaultValue = "/") String path, @Name(value = "simple", defaultValue = "false") boolean simpleMode) throws Exception {
        return xmlXpathToMapResult(url, simpleMode, path);
    }

    @Procedure(deprecatedBy = "apoc.load.xml")
    @Deprecated
    @Description("apoc.load.xmlSimple('http://example.com/test.xml') YIELD value as doc CREATE (p:Person) SET p.name = doc.name load from XML URL (e.g. web-api) to import XML as single nested map with attributes and _type, _text and _children fields. This method does intentionally not work with XML mixed content.")
    public Stream<MapResult> xmlSimple(@Name("url") String url) throws Exception {
        return xmlToMapResult(url, true);
    }

    private Stream<MapResult> xmlXpathToMapResult(@Name("url") String url, boolean simpleMode, String path) throws Exception {

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);
        documentBuilderFactory.setIgnoringElementContentWhitespace(true);
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();

        FileUtils.checkReadAllowed(url);
        URLConnection urlConnection = new URL(url).openConnection();
        Document doc = documentBuilder.parse(urlConnection.getInputStream());

        XPathFactory xPathFactory = XPathFactory.newInstance();

        XPath xPath = xPathFactory.newXPath();

        path = StringUtils.isEmpty(path) ? "/" : path;
        XPathExpression xPathExpression = xPath.compile(path);
        NodeList nodeList = (NodeList) xPathExpression.evaluate(doc, XPathConstants.NODESET);

        List<MapResult> result = new ArrayList<>();

        for (int i = 0; i < nodeList.getLength(); i++) {
            final Deque<Map<String, Object>> stack = new LinkedList<>();

            handleNode(stack, nodeList.item(i), simpleMode);
            for (int index = 0; index < stack.size(); index++) {
                result.add(new MapResult(stack.pollFirst()));
            }
        }
        return result.stream();
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

    private XMLStreamReader getXMLStreamReaderFromUrl(String url) throws IOException, XMLStreamException {
        FileUtils.checkReadAllowed(url);
        URLConnection urlConnection = new URL(url).openConnection();
        FACTORY.setProperty(XMLInputFactory.IS_COALESCING, true);
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
                elementMap = new LinkedHashMap<>(attributes + 3);
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
                if (children != null) {
                    if ((children instanceof String) || collectionIsAllStrings(children)) {
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

    private void handleNode(Deque<Map<String, Object>> stack, Node node, boolean simpleMode) {

        // Handle document node
        if (node.getNodeType() == Node.DOCUMENT_NODE) {
            NodeList children = node.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                if (children.item(i).getLocalName() != null) {
                    handleNode(stack, children.item(i), simpleMode);
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
            Node child = children.item(i);

            // This is to deal with text between xml tags for example new line characters
            if (child.getNodeType() != Node.TEXT_NODE && child.getNodeType() != Node.CDATA_SECTION_NODE) {
                handleNode(stack, child, simpleMode);
                count++;
            } else {
                // Deal with text nodes
                handleTextNode(child, elementMap);
            }
        }

        if (children.getLength() > 1) {
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
     * Collects type and attributes for the node
     *
     * @param node
     * @param elementMap
     */
    private void handleTypeAndAttributes(Node node, Map<String, Object> elementMap) {
        // Set type
        if (node.getLocalName() != null) {
            elementMap.put("_type", node.getLocalName());
        }

        // Set the attributes
        if (node.getAttributes() != null) {
            NamedNodeMap attributeMap = node.getAttributes();
            for (int i = 0; i < attributeMap.getLength(); i++) {
                Node attribute = attributeMap.item(i);
                elementMap.put(attribute.getNodeName(), attribute.getNodeValue());
            }
        }
    }

    /**
     * Handle TEXT nodes and CDATA nodes
     *
     * @param node
     * @param elementMap
     */
    private void handleTextNode(Node node, Map<String, Object> elementMap) {
        Object text = "";
        int nodeType = node.getNodeType();
        switch (nodeType) {
            case Node.TEXT_NODE:
                text = normalizeText(node.getNodeValue());
                break;
            case Node.CDATA_SECTION_NODE:
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
    private String normalizeText(String text) {
        String[] tokens = StringUtils.split(text, "\n");
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim();
        }

        return StringUtils.join(tokens, " ").trim();
    }

    private boolean collectionIsAllStrings(Object collection) {
        if (collection instanceof Collection) {
            return ((Collection<Object>) collection).stream().allMatch(o -> o instanceof String);
        } else {
            return false;
        }
    }

    private void amendToList(Map<String, Object> map, String key, Object value) {
        final Object element = map.get(key);
        if (element == null) {
            map.put(key, value);
        } else {
            if (element instanceof List) {
                ((List) element).add(value);
            } else {
                List<Object> list = new LinkedList<>();
                list.add(element);
                list.add(value);
                map.put(key, list);
            }
        }
    }

    @Procedure(mode = Mode.WRITE, value = "apoc.xml.import")
    public Stream<NodeResult> importToGraph(@Name("url") String url, @Name(value="config", defaultValue = "{}") Map<String, String> config) throws IOException, XMLStreamException {
        final XMLStreamReader xml = getXMLStreamReaderFromUrl(url);

        //TODO: make labels, reltypes and magic properties configurable

        Deque<org.neo4j.graphdb.Node> parents = new ArrayDeque<>();
        org.neo4j.graphdb.Node root = db.createNode(Label.label("XmlDocument"));;
        root.setProperty("_xmlVersion", xml.getVersion());
        root.setProperty("_xmlEncoding", xml.getEncoding());
        root.setProperty("url", url);
        parents.push(root);
        org.neo4j.graphdb.Node last = root;

        while (xml.hasNext()) {
            xml.next();

            switch (xml.getEventType()) {
                case XMLStreamConstants.START_DOCUMENT:
                    // xmlsteamreader starts off by definition at START_DOCUMENT prior to call next() - so ignore this one
                    break;

                case XMLStreamConstants.PROCESSING_INSTRUCTION:
                    org.neo4j.graphdb.Node pi = db.createNode(Label.label("XmlProcessingInstruction"));
                    pi.setProperty("_piData", xml.getPIData());
                    pi.setProperty("_piTarget", xml.getPITarget());
                    last.createRelationshipTo(pi, RelationshipType.withName("NEXT"));
                    pi.createRelationshipTo(parents.peek(), RelationshipType.withName("IS_CHILD_OF"));
                    last = pi;
                    break;

                case XMLStreamConstants.START_ELEMENT:
                    final QName qName = xml.getName();
                    final org.neo4j.graphdb.Node tag = db.createNode(Label.label("XmlTag"));
                    tag.setProperty("_name", qName.getLocalPart());
                    for (int i=0; i<xml.getAttributeCount(); i++) {
                        tag.setProperty(xml.getAttributeLocalName(i), xml.getAttributeValue(i));
                    }
                    last.createRelationshipTo(tag, RelationshipType.withName("NEXT"));
                    tag.createRelationshipTo(parents.peek(), RelationshipType.withName("IS_CHILD_OF"));
                    last = tag;
                    parents.push(tag);
                    break;

                case XMLStreamConstants.CHARACTERS:
                    String text = xml.getText().trim();
                    if (!text.isEmpty()) {
                        String[] words = text.split("\\s");
                        for (int i=0; i<words.length; i++) {
                            org.neo4j.graphdb.Node word = db.createNode(Label.label("XmlWord"));
                            word.setProperty("text", words[i]);
                            last.createRelationshipTo(word, RelationshipType.withName("NEXT"));
                            word.createRelationshipTo(parents.peek(), RelationshipType.withName("IS_CHILD_OF"));
                            last = word;
                        }
                    }

                    break;

                case XMLStreamConstants.END_ELEMENT:
                    parents.pop();
                    break;

                case XMLStreamConstants.END_DOCUMENT:
                    parents.pop();
                    break;
                default:
                    throw new UnsupportedOperationException("dunno how deal with xml event type " + xml.getEventType());
            }

        }
        if (!parents.isEmpty()) {
            throw new IllegalStateException("non empty parents");
        }
        return Stream.of(new NodeResult(root));
    }
}
