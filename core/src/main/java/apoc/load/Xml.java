package apoc.load;

import apoc.ApocConfig;
import apoc.export.util.CountingInputStream;
import apoc.generate.config.InvalidConfigException;
import apoc.result.MapResult;
import apoc.result.NodeResult;
import apoc.util.FileUtils;
import apoc.util.Util;
import jdk.jfr.MemoryAddress;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.w3c.dom.CharacterData;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

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
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static apoc.util.Util.cleanUrl;
import static javax.xml.stream.XMLStreamConstants.*;

public class Xml {

    private static final XMLInputFactory FACTORY = XMLInputFactory.newFactory();

    @Context
    public ApocConfig apocConfig;

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Procedure
    @Description("apoc.load.xml('http://example.com/test.xml', 'xPath',config, false) YIELD value as doc CREATE (p:Person) SET p.name = doc.name - load from XML URL (e.g. web-api) to import XML as single nested map with attributes and _type, _text and _childrenx fields.")
    public Stream<MapResult> xml(@Name("url") String url, @Name(value = "path", defaultValue = "/") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config, @Name(value = "simple", defaultValue = "false") boolean simpleMode) throws Exception {
        return xmlXpathToMapResult(url, simpleMode, path ,config);
    }

    @UserFunction("apoc.xml.parse")
    @Description("RETURN apoc.xml.parse(<xml string>, <xPath string>, config, false) AS value")
    public Map<String, Object> parse(@Name("data") String data, @Name(value = "path", defaultValue = "/") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config, @Name(value = "simple", defaultValue = "false") boolean simpleMode) throws Exception {
        if (config == null) config = Collections.emptyMap();
        boolean failOnError = (boolean) config.getOrDefault("failOnError", true);
        return parse(new ByteArrayInputStream(data.getBytes(Charset.forName("UTF-8"))), simpleMode, path, failOnError)
                .map(mr -> mr.value).findFirst().orElse(null);
    }

    @Procedure(deprecatedBy = "apoc.load.xml")
    @Deprecated
    @Description("apoc.load.xmlSimple('http://example.com/test.xml') YIELD value as doc CREATE (p:Person) SET p.name = doc.name - load from XML URL (e.g. web-api) to import XML as single nested map with attributes and _type, _text and _children fields. This method does intentionally not work with XML mixed content.")
    public Stream<MapResult> xmlSimple(@Name("url") String url) throws Exception {
        return xmlToMapResult(url, true);
    }

    private Stream<MapResult> xmlXpathToMapResult(@Name("url") String url, boolean simpleMode, String path, Map<String, Object> config) throws Exception {
        if (config == null) config = Collections.emptyMap();
        boolean failOnError = (boolean) config.getOrDefault("failOnError", true);
        try {
            apocConfig.checkReadAllowed(url);
            url = FileUtils.changeFileUrlIfImportDirectoryConstrained(url);
            Map<String, Object> headers = (Map) config.getOrDefault( "headers", Collections.emptyMap() );
            CountingInputStream is = Util.openInputStream(url, headers, null);
            return parse(is, simpleMode, path, failOnError);
        } catch (Exception e){
            if(!failOnError)
                return Stream.of(new MapResult(Collections.emptyMap()));
            else
                throw e;
        }
    }

    private Stream<MapResult> parse(InputStream data, boolean simpleMode, String path, boolean failOnError) throws Exception {
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

                handleNode(stack, nodeList.item(i), simpleMode);
                for (int index = 0; index < stack.size(); index++) {
                    result.add(new MapResult(stack.pollFirst()));
                }
            }
        }
        catch (FileNotFoundException e){
            if(!failOnError)
                return Stream.of(new MapResult(Collections.emptyMap()));
            else
                throw e;
        }
        catch (Exception e){
            if(!failOnError)
                return Stream.of(new MapResult(Collections.emptyMap()));
            else
                throw e;
        }
        return result.stream();
    }

    private Stream<MapResult> xmlToMapResult(@Name("url") String url, boolean simpleMode) {
        try {
            XMLStreamReader reader = getXMLStreamReaderFromUrl(url, new XmlImportConfig(Collections.EMPTY_MAP));
            final Deque<Map<String, Object>> stack = new LinkedList<>();
            do {
                handleXmlEvent(stack, reader, simpleMode);
            } while (proceedReader(reader));

            return Stream.of(new MapResult(stack.getFirst()));
        } catch (IOException | XMLStreamException e) {
            throw new RuntimeException("Can't read url " + cleanUrl(url) + " as XML", e);
        }
    }

    private XMLStreamReader getXMLStreamReaderFromUrl(String url, XmlImportConfig config) throws IOException, XMLStreamException {
        apocConfig.checkReadAllowed(url);
        url = FileUtils.changeFileUrlIfImportDirectoryConstrained(url);
        URLConnection urlConnection = new URL(url).openConnection();
        FACTORY.setProperty(XMLInputFactory.IS_COALESCING, true);
        InputStream inputStream = urlConnection.getInputStream();

        if (config.isFilterLeadingWhitespace()) {
            inputStream = new SkipWhitespaceInputStream(inputStream);
        }
        return FACTORY.createXMLStreamReader(inputStream);
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

    public static class ParentAndChildPair {
        private org.neo4j.graphdb.Node parent;
        private org.neo4j.graphdb.Node previousChild=null;

        public ParentAndChildPair(org.neo4j.graphdb.Node parent) {
            this.parent = parent;
        }

        public org.neo4j.graphdb.Node getParent() {
            return parent;
        }

        public void setParent(org.neo4j.graphdb.Node parent) {
            this.parent = parent;
        }

        public org.neo4j.graphdb.Node getPreviousChild() {
            return previousChild;
        }

        public void setPreviousChild(org.neo4j.graphdb.Node previousChild) {
            this.previousChild = previousChild;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ParentAndChildPair that = (ParentAndChildPair) o;
            return parent.equals(that.parent);
        }

        @Override
        public int hashCode() {
            return parent.hashCode();
        }
    }

    private static class XmlImportConfig {

        private boolean connectCharacters;
        private Pattern delimiter;
        private Label label = Label.label("XmlCharacters");
        private RelationshipType relType = RelationshipType.withName("NE");
        private Map<String, String> charactersForTag = new HashMap<>();
        final private boolean filterLeadingWhitespace;

        public XmlImportConfig(Map<String, Object> config) {
            connectCharacters = BooleanUtils.toBoolean((Boolean) config.get("connectCharacters"));
            filterLeadingWhitespace = BooleanUtils.toBoolean((Boolean) config.get("filterLeadingWhitespace"));

            String _delimiter = (String) config.get("delimiter");
            if (_delimiter != null) {
                connectCharacters = true;
            }
            delimiter = Pattern.compile(_delimiter == null ? "\\s" : _delimiter);

            String _label = (String) config.get("label");
            if (_label != null) {
                label = Label.label(_label);
                connectCharacters = true;
            }

            String _relType = (String) config.get("relType");
            if (_relType != null) {
                relType = RelationshipType.withName(_relType);
                connectCharacters = true;
            }

            Map<String,String> _charactersForTag = (Map<String, String>) config.get("charactersForTag");
            if (_charactersForTag !=null) {
                charactersForTag = _charactersForTag;
            }

            if (config.containsKey("createNextWordRelationships")) {
                throw new InvalidConfigException("usage of `createNextWordRelationships` is no longer allowed. Use `{relType:'NEXT_WORD', label:'XmlWord'}` instead.");
            }
        }

        public Pattern getDelimiter() {
            return delimiter;
        }

        public Label getLabel() {
            return label;
        }

        public RelationshipType getRelType() {
            return relType;
        }

        public boolean isConnectCharacters() {
            return connectCharacters;
        }

        public Map<String, String> getCharactersForTag() {
            return charactersForTag;
        }

        public boolean isFilterLeadingWhitespace() {
            return filterLeadingWhitespace;
        }

    }

    private static class ImportState {
        private final Deque<ParentAndChildPair> parents = new ArrayDeque<>();
        private org.neo4j.graphdb.Node last;
        private org.neo4j.graphdb.Node lastWord;
        private int currentCharacterIndex = 0;

        public ImportState(org.neo4j.graphdb.Node initialNode) {
            this.last = initialNode;
            this.lastWord = initialNode;
        }

        public void push(ParentAndChildPair parentAndChildPair) {
            parents.push(parentAndChildPair);
        }

        public org.neo4j.graphdb.Node getLastWord() {
            return lastWord;
        }

        public void setLastWord(org.neo4j.graphdb.Node lastWord) {
            this.lastWord = lastWord;
        }

        public int getCurrentCharacterIndex() {
            return currentCharacterIndex;
        }

        public ParentAndChildPair pop() {
            return parents.pop();
        }

        public boolean isEmpty() {
            return parents.isEmpty();
        }

        public void updateLast(org.neo4j.graphdb.Node thisNode) {
            ParentAndChildPair parentAndChildPair = parents.peek();
            final org.neo4j.graphdb.Node parent = parentAndChildPair.getParent();
            final org.neo4j.graphdb.Node previousChild = parentAndChildPair.getPreviousChild();

            last.createRelationshipTo(thisNode, RelationshipType.withName("NEXT"));
            thisNode.createRelationshipTo(parent, RelationshipType.withName("IS_CHILD_OF"));
            if (previousChild ==null) {
                thisNode.createRelationshipTo(parent, RelationshipType.withName("FIRST_CHILD_OF"));
            } else {
                previousChild.createRelationshipTo(thisNode, RelationshipType.withName("NEXT_SIBLING"));
            }
            parentAndChildPair.setPreviousChild(thisNode);
            last = thisNode;
        }

        public void addCurrentCharacterIndex(int length) {
            currentCharacterIndex += length;
        }
    }

    @Procedure(mode = Mode.WRITE, value = "apoc.xml.import")
    @Deprecated
    @Description("Deprecated by apoc.import.xml")
    public Stream<NodeResult> importToGraphDeprecated(@Name("url") String url, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException, XMLStreamException {
        return importToGraph(url, config);
    }

    @Procedure(mode = Mode.WRITE, value = "apoc.import.xml")
    @Description("apoc.import.xml(file,config) - imports graph from provided file")
    public Stream<NodeResult> importToGraph(@Name("url") String url, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException, XMLStreamException {
        XmlImportConfig importConfig = new XmlImportConfig(config);
        //TODO: make labels, reltypes and magic properties configurable

        final XMLStreamReader xml = getXMLStreamReaderFromUrl(url, importConfig);

        // stores parents and their most recent child
        org.neo4j.graphdb.Node root = tx.createNode(Label.label("XmlDocument"));
        setPropertyIfNotNull(root, "_xmlVersion", xml.getVersion());
        setPropertyIfNotNull(root, "_xmlEncoding", xml.getEncoding());
        root.setProperty("url", url);

        ImportState state = new ImportState(root);
        state.push(new ParentAndChildPair(root));

        while (xml.hasNext()) {
            xml.next();

            switch (xml.getEventType()) {
                case XMLStreamConstants.START_DOCUMENT:
                    // xmlsteamreader starts off by definition at START_DOCUMENT prior to call next() - so ignore this one
                    break;

                case XMLStreamConstants.PROCESSING_INSTRUCTION:
                    org.neo4j.graphdb.Node pi = tx.createNode(Label.label("XmlProcessingInstruction"));
                    pi.setProperty("_piData", xml.getPIData());
                    pi.setProperty("_piTarget", xml.getPITarget());
                    state.updateLast(pi);
                    break;

                case XMLStreamConstants.START_ELEMENT:
                    final QName qName = xml.getName();
                    final org.neo4j.graphdb.Node tag = tx.createNode(Label.label("XmlTag"));
                    tag.setProperty("_name", qName.getLocalPart());
                    for (int i=0; i<xml.getAttributeCount(); i++) {
                        tag.setProperty(xml.getAttributeLocalName(i), xml.getAttributeValue(i));
                    }

                    state.updateLast(tag);
                    state.push(new ParentAndChildPair(tag));
                    break;

                case XMLStreamConstants.CHARACTERS:
                    List<String> words = parseTextIntoPartsAndDelimiters(xml.getText(), importConfig.getDelimiter());
                    for (String currentWord : words) {
                        createCharactersNode(currentWord, state, importConfig);
                    }
                    break;

                case XMLStreamConstants.END_ELEMENT:

                    String charactersForTag = importConfig.getCharactersForTag().get(xml.getName().getLocalPart());
                    if (charactersForTag!=null) {
                        createCharactersNode(charactersForTag, state, importConfig);
                    }
                    ParentAndChildPair parent = state.pop();
                    if (parent.getPreviousChild()!=null) {
                        parent.getPreviousChild().createRelationshipTo(parent.getParent(), RelationshipType.withName("LAST_CHILD_OF"));
                    }
                    break;

                case XMLStreamConstants.END_DOCUMENT:
                    state.pop();
                    break;

                case XMLStreamConstants.COMMENT:
                case XMLStreamConstants.SPACE:
                    // intentionally do nothing
                    break;
                default:
                    log.warn("xml file contains a {} type structure - ignoring this.", xml.getEventType());
            }

        }
        if (!state.isEmpty()) {
            throw new IllegalStateException("non empty parents, this indicates a bug");
        }
        return Stream.of(new NodeResult(root));
    }

    private void createCharactersNode(String currentWord, ImportState state, XmlImportConfig importConfig) {
        org.neo4j.graphdb.Node word = tx.createNode(importConfig.getLabel());
        word.setProperty("text", currentWord);
        word.setProperty("startIndex", state.getCurrentCharacterIndex());
        state.addCurrentCharacterIndex(currentWord.length());
        word.setProperty("endIndex", state.getCurrentCharacterIndex() - 1);

        state.updateLast(word);
        if (importConfig.isConnectCharacters()) {
            state.getLastWord().createRelationshipTo(word, importConfig.getRelType());
            state.setLastWord(word);
        }
    }

    List<String> parseTextIntoPartsAndDelimiters(String sourceString, Pattern delimiterPattern) {
        Matcher matcher = delimiterPattern.matcher(sourceString);
        ArrayList<String> result = new ArrayList<>();

        int prevEndIndex = 0;
        int length = sourceString.length();
        while (matcher.find()) {
            int start = matcher.start();
            int end = matcher.end();
            if (prevEndIndex != start) {
                result.add(sourceString.substring(prevEndIndex, start));
            }
            result.add(sourceString.substring(start, end));
            prevEndIndex = end;
        }
        if (prevEndIndex!=length) {
            result.add(sourceString.substring(prevEndIndex, length));

        }
        return result;
    }

    private void setPropertyIfNotNull(org.neo4j.graphdb.Node root, String propertyKey, Object value) {
        if (value!=null) {
            root.setProperty(propertyKey, value);
        }
    }

}
