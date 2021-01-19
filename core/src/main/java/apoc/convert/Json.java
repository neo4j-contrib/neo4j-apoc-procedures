package apoc.convert;

import apoc.export.json.JsonFormatSerializer;
import apoc.export.util.ExportConfig;
import apoc.export.util.FormatUtils;
import apoc.export.util.Reporter;
import apoc.meta.Meta;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.util.FormatUtils.getLabelsSorted;
import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

public class Json {

    // TODO TODOISSIMO - USARE TOMAP DI FORMATUTILS!!!

    private Object writeJsonResult(Object value) {
        Meta.Types type = Meta.Types.of(value);
        switch (type) {
            case NODE:
//                writeFieldName(jsonGenerator, keyName, writeKey);
                return nodeToMap((Node) value, true);
//                break;
            case RELATIONSHIP:
//                writeFieldName(jsonGenerator, keyName, writeKey);
                return relToMap((Relationship) value);
//                break;
            case PATH:
                return writeJsonResult(StreamSupport.stream(((Path)value).spliterator(),false).map(i-> i instanceof Node ? nodeToMap(i, true) : relToMap(i)).collect(Collectors.toList()));
//                writeFieldName(jsonGenerator, keyName, writeKey);
//                writePath((Path) value);
//                return "";
//                break;
            case LIST:
//                Object[] list = value.getClass().isArray() ? (Object[]) value : ((List<Object>) value).toArray();
//                return Arrays.stream(arrayPattern.split(value)).map(this::convertType).collect(Collectors.toList());
                return ((ArrayList) value).stream().map(this::writeJsonResult).collect(Collectors.toList());
            case MAP:
                return ((Map<String, Object>) value).entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> writeJsonResult(e.getValue())));
            default:
                return value;
        }
    }

    public static Map<String,Object> relToMap(Entity pc) {
        Relationship rel = (Relationship) pc;

        // todo - farlo come Node, cioè emptyMap e poi popolare
        Map<String, Object> mapRel = map(
                "id", String.valueOf(rel.getId()),
                "type", "relationship",
                "label", rel.getType().toString(),
                "start", nodeToMap(rel.getStartNode(), false),
                "«end", nodeToMap(rel.getEndNode(), false));

        // todo - rel.getType().toString() e rel.getType().name() sono la stessa cosa??

        // todo - labels metterlo solo se ci sono labels nel risultato...

        // todo - forse basta map("properties", props) se isEmpty?
        Map<String, Object> props = pc.getAllProperties();
        if (!props.isEmpty()) {
            mapRel.put("properties", props);
        }
//        }
        // todo - if properties is empty non lo metto se isConvertToMap.
        // todo - if more info è true metto in start ed end anche le labels
        // todo - if more info è true metto  anche type: relationship
        // todo - if more info è true metto anche le labels - jsonGenerator.writeStringField("label", rel.getType().toString());

        return mapRel;
//            return map("id", rel.getId(), "type", rel.getType().name(),
//                    "start", rel.getStartNode().getId(),"end", rel.getEndNode().getId(),
//                    "properties",pc.getAllProperties());
    }

    public static Map<String,Object> nodeToMap(Entity pc, boolean mapForNode) {

        Node node = (Node) pc;
        long id = node.getId();
        // todo - if properties is empty non lo metto se isConvertToMap.
        // todo - if more info è true metto metto anche type: node
        Map<String, Object> mapNode = map("id", String.valueOf(id));

        Map<String, Object> props = pc.getAllProperties();
        // todo - forse basta map("properties", props) se isEmpty?
//        if(isConvertToMap) {
//            mapNode.putAll(map("id", String.valueOf(id), "type", "node"));
            if (!props.isEmpty()) {
                mapNode.put("properties", props);
            }
            if (node.getLabels().iterator().hasNext()) {
                mapNode.put("labels", labelStrings(node));
            }
            if (mapForNode) {
                mapNode.put( "type", "node");
            }
//        }

        return mapNode;

    }


    public void writeRelationship(JsonGenerator jsonGenerator, Relationship rel, ExportConfig config) throws IOException {
        Node startNode = rel.getStartNode();
        Node endNode = rel.getEndNode();
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("id", String.valueOf(rel.getId()));
        jsonGenerator.writeStringField("type", "relationship");
        jsonGenerator.writeStringField("label", rel.getType().toString());
        serializeProperties(jsonGenerator, rel.getAllProperties());
        writeRelationshipNode(jsonGenerator, "start", startNode, config);
        writeRelationshipNode(jsonGenerator, "end", endNode, config);
        jsonGenerator.writeEndObject();
    }

    public void serializeProperties(JsonGenerator jsonGenerator, Map<String, Object> properties) throws IOException {
        if(properties != null && !properties.isEmpty()) {
            jsonGenerator.writeObjectFieldStart("properties");
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                serializeProperty(jsonGenerator, key, value, true);
            }
            jsonGenerator.writeEndObject();
        }
    }

    public void serializeProperty(JsonGenerator jsonGenerator, String key, Object value, boolean writeKey) throws IOException {
        if (value == null) {
            if (writeKey) {
                jsonGenerator.writeNullField(key);
            } else {
                jsonGenerator.writeNull();
            }
        } else {
            if (writeKey) {
                jsonGenerator.writeObjectField(key, value);
            } else {
                jsonGenerator.writeObject(value);
            }
        }
    }

    private void writeNodeDetails(JsonGenerator jsonGenerator, Node node, boolean withNodeProperties) throws IOException {
        jsonGenerator.writeStringField("id", String.valueOf(node.getId()));

        if (node.getLabels().iterator().hasNext()) {
            jsonGenerator.writeArrayFieldStart("labels");

            List<String> labels = getLabelsSorted(node);
            for (String label : labels) {
                jsonGenerator.writeString(label);
            }
            jsonGenerator.writeEndArray();
        }
        if (withNodeProperties) {
            serializeProperties(jsonGenerator, node.getAllProperties());
        }
    }

    private void writeRelationshipNode(JsonGenerator jsonGenerator, String type, Node node, ExportConfig config) throws IOException {
        jsonGenerator.writeObjectFieldStart(type);

        writeNodeDetails(jsonGenerator, node, config.writeNodeProperties());
        jsonGenerator.writeEndObject();
    }


    private void writePath(Path path) throws IOException {
//        jsonGenerator.writeStartObject();
//        jsonGenerator.writeObjectField("length", path.length());
//        jsonGenerator.writeArrayFieldStart("rels");
//        writeRels(path.relationships(), reporter, jsonGenerator, config);
//        jsonGenerator.writeEndArray();
//        jsonGenerator.writeArrayFieldStart("nodes");
//        writeNodes(path.nodes(), reporter, jsonGenerator, config);
//        jsonGenerator.writeEndArray();
//        jsonGenerator.writeEndObject();
    }

    private void writeNodes(Iterable<Node> nodes) throws IOException {
        for (Node node : nodes) {
            writeNode(node);
        }
    }

    private Map<String, Object> writeNode(Node node) {
        Map<String, Object> nodeMap = new HashMap<>(Map.of(
                "id", String.valueOf(node.getId()),
                "labels", node.getLabels(),
                "type", "node"));
        // todo - fare l'if anche per le labels??
        Map<String, Object> properties = node.getAllProperties();
        if (properties.isEmpty()) {
            nodeMap.put("properties", properties);
        }
        return nodeMap;
//
////        writeJsonIdKeyStart(jsonGenerator, node.getId());
//
//        writeNode(jsonGenerator, node, config);
//
//        reporter.update(1, 0, allProperties.size());
    }

//    private void writeJsonIdKeyStart(JsonGenerator jsonGenerator, long id) throws IOException {
//        if (!isExportSubGraph) {
//            return;
//        }
//        switch (format) {
//            case JSON_ID_AS_KEYS:
//                writeFieldName(jsonGenerator, String.valueOf(id), true);
//                break;
//        }
//    }

    private void writeRels(Iterable<Relationship> rels) throws IOException {
        for (Relationship rel : rels) {
            writeRel(rel);
        }
    }

    private void writeRel(Relationship rel) throws IOException {
        Map<String, Object> allProperties = rel.getAllProperties();

//        writeJsonIdKeyStart(jsonGenerator, rel.getId());

//        writeRelationship(jsonGenerator, rel, config);

//        reporter.update(0, 1, allProperties.size());
    }

//    private String writeJsonResult(Object value) throws IOException {
//        return JsonUtil.OBJECT_MAPPER.writeValueAsString(value);
//        jsonGenerator.writeStartObject();
//        for (int col = 0; col < header.length; col++) {
//            String keyName = header[col];
//            Object value = row.get(keyName);
//            // TODO - VEDERE QUA CHE SUCCEDE
//            write(reporter, jsonGenerator, config, keyName, value, true);
//        }
//        jsonGenerator.writeEndObject();
//    }


    @Context
    public org.neo4j.graphdb.GraphDatabaseService db;

    @Context public Log log;

    @UserFunction("apoc.json.path")
    @Description("apoc.json.path('{json}','json-path')")
    public Object path(@Name("json") String json, @Name(value = "path",defaultValue = "$") String path) {
        return JsonUtil.parse(json,path,Object.class);
    }
    @UserFunction("apoc.convert.toJson")
    @Description("apoc.convert.toJson([1,2,3]) or toJson({a:42,b:\"foo\",c:[1,2,3]})")
    public String toJson(@Name("value") Object value) throws JsonProcessingException {
        log.info("osvaldo json string");
//        log.info("string " + JsonUtil.OBJECT_MAPPER.writeValueAsString(value));



//        if (value instanceof Node) {
//            value = ((Node)value).getAllProperties();
//        }
        // rel...

        //path...

        // todo - intercettare il punto in cui apoc.export.json.query scrive nel file

        // todo - aggiungere "type": "node" ... , relazioni con nodeStart/end con più info (labels...), ed id stringa e non numerico
        // ossia fare un overload...

        // todo - vedere che stampa Meta.Types.of(value)
//        return FormatUtils.toString(value, true);
        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(writeJsonResult(value));
        } catch (IOException e) {
            log.info("osvaldo error");
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure(mode = Mode.WRITE) // ,name = "apoc.json.setProperty")
    @Description("apoc.convert.setJsonProperty(node,key,complexValue) - sets value serialized to JSON as property with the given name on the node")
    public void setJsonProperty(@Name("node") Node node, @Name("key") String key, @Name("value") Object value) {
        try {
            node.setProperty(key, JsonUtil.OBJECT_MAPPER.writeValueAsString(value));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @UserFunction// ("apoc.json.getJsonProperty")
    @Description("apoc.convert.getJsonProperty(node,key[,'json-path']) - converts serialized JSON in property back to original object")
    public Object getJsonProperty(@Name("node") Node node, @Name("key") String key,@Name(value = "path",defaultValue = "") String path) {
        String value = (String) node.getProperty(key, null);
        return JsonUtil.parse(value, path, Object.class);
    }

    @UserFunction// ("apoc.json.getJsonPropertyMap")
    @Description("apoc.convert.getJsonPropertyMap(node,key[,'json-path']) - converts serialized JSON in property back to map")
    public Map<String,Object> getJsonPropertyMap(@Name("node") Node node, @Name("key") String key,@Name(value = "path",defaultValue = "") String path) {
        String value = (String) node.getProperty(key, null);
        return JsonUtil.parse(value, path, Map.class);
    }

    @UserFunction
    @Description("apoc.convert.fromJsonMap('{\"a\":42,\"b\":\"foo\",\"c\":[1,2,3]}'[,'json-path'])")
    public Map<String,Object> fromJsonMap(@Name("map") String value,@Name(value = "path",defaultValue = "") String path) {
        return JsonUtil.parse(value, path, Map.class);
    }

    @UserFunction
    @Description("apoc.convert.fromJsonList('[1,2,3]'[,'json-path'])")
    public List<Object> fromJsonList(@Name("list") String value, @Name(value = "path",defaultValue = "") String path) {
        return JsonUtil.parse(value, path, List.class);
    }

    @Procedure("apoc.convert.toTree")
    @Description("apoc.convert.toTree([paths],[lowerCaseRels=true], [config]) creates a stream of nested documents representing the at least one root of these paths")
    // todo optinally provide root node
    public Stream<MapResult> toTree(@Name("paths") List<Path> paths, @Name(value = "lowerCaseRels",defaultValue = "true") boolean lowerCaseRels, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (paths.isEmpty()) return Stream.of(new MapResult(Collections.emptyMap()));
        ConvertConfig conf = new ConvertConfig(config);
        Map<String, List<String>> nodes = conf.getNodes();
        Map<String, List<String>> rels = conf.getRels();

        Map<Long, Map<String, Object>> maps = new HashMap<>(paths.size() * 100);
        for (Path path : paths) {
            Iterator<Entity> it = path.iterator();
            while (it.hasNext()) {
                Node n = (Node) it.next();
                Map<String, Object> nMap = maps.computeIfAbsent(n.getId(), (id) -> toMap(n, nodes));
                if (it.hasNext()) {
                    Relationship r = (Relationship) it.next();
                    Node m = r.getOtherNode(n);
                    String typeName = lowerCaseRels ? r.getType().name().toLowerCase() : r.getType().name();
                    // todo take direction into account and create collection into outgoing direction ??
                    // parent-[:HAS_CHILD]->(child) vs. (parent)<-[:PARENT_OF]-(child)
                    if (!nMap.containsKey(typeName)) nMap.put(typeName, new ArrayList<>(16));
                    List<Map<String, Object>> list = (List) nMap.get(typeName);
                    Optional<Map<String, Object>> optMap = list.stream()
                            .filter(elem -> elem.get("_id").equals(m.getId()))
                            .findFirst();
                    if (!optMap.isPresent()) {
                        Map<String, Object> mMap = toMap(m, nodes);
                        mMap = addRelProperties(mMap, typeName, r, rels);
                        maps.put(m.getId(), mMap);
                        list.add(maps.get(m.getId()));
                    }
                }
            }
        }
        return paths.stream()
                .map(Path::startNode)
                .distinct()
                .map(n -> maps.remove(n.getId()))
                .map(m -> m == null ? Collections.<String,Object>emptyMap() : m)
                .map(MapResult::new);
    }

    @UserFunction("apoc.convert.toSortedJsonMap")
    @Description("apoc.convert.toSortedJsonMap(node|map, ignoreCase:true) - returns a JSON map with keys sorted alphabetically, with optional case sensitivity")
    public String toSortedJsonMap(@Name("value") Object value, @Name(value="ignoreCase", defaultValue = "true") boolean ignoreCase) {
        Map<String, Object> inputMap;
        Map<String, Object> sortedMap;

        if (value instanceof Node) {
            inputMap = ((Node)value).getAllProperties();
        } else if (value instanceof Map) {
            inputMap = (Map<String, Object>) value;
        } else {
            throw new IllegalArgumentException("input value must be a Node or a map");
        }

        if (ignoreCase) {
            sortedMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            sortedMap.putAll(inputMap);
        } else {
            sortedMap = new TreeMap<>(inputMap);
        }

        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(sortedMap);
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    private Map<String, Object> addRelProperties(Map<String, Object> mMap, String typeName, Relationship r, Map<String, List<String>> relFilters) {
        Map<String, Object> rProps = r.getAllProperties();
        if (rProps.isEmpty()) return mMap;
        String prefix = typeName + ".";
        if (relFilters.containsKey(typeName)) {
            rProps = filterProperties(rProps, relFilters.get(typeName));
        }
        rProps.forEach((k, v) -> mMap.put(prefix + k, v));
        return mMap;
    }

    private Map<String, Object> toMap(Node n, Map<String, List<String>> nodeFilters) {
        Map<String, Object> props = n.getAllProperties();
        Map<String, Object> result = new LinkedHashMap<>(props.size() + 2);
        String type = Util.labelString(n);
        result.put("_id", n.getId());
        result.put("_type", type);
        if (nodeFilters.containsKey(type)){ //Check if list contains LABEL
            props = filterProperties(props, nodeFilters.get(type));
        }
        result.putAll(props);
        return result;
    }

    private Map<String, Object> filterProperties(Map<String, Object> props, List<String> filters) {
        boolean isExclude = filters.get(0).startsWith("-");

        return props.entrySet().stream().filter(e -> isExclude ? !filters.contains("-" + e.getKey()) : filters.contains(e.getKey())).collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue()));
    }

}
