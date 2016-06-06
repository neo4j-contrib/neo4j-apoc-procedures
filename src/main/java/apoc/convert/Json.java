package apoc.convert;

import java.util.*;
import java.io.*;
import java.util.stream.*;

import apoc.Description;
import apoc.result.ListResult;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.result.StringResult;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import static java.util.Arrays.asList;

public class Json {

    @Context
    public org.neo4j.graphdb.GraphDatabaseService db;

    @Procedure
    @Description("apoc.convert.toJson([1,2,3]) or toJson({a:42,b:\"foo\",c:[1,2,3]})")
    public Stream<StringResult> toJson(@Name("value") Object value) {
        try {
            return Stream.of(new StringResult(JsonUtil.OBJECT_MAPPER.writeValueAsString(value)));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure
    @PerformsWrites
    @Description("apoc.json.setJsonProperty(node,key,complexValue) - sets value serialized to JSON as property with the given name on the node")
    public void setJsonProperty(@Name("node") Node node, @Name("key") String key, @Name("value") Object value) {
        try {
            node.setProperty(key, JsonUtil.OBJECT_MAPPER.writeValueAsString(value));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure
    @Description("apoc.json.getJsonProperty(node,key) - converts serialized JSON in property to structure again")
    public Stream<ObjectResult> getJsonProperty(@Name("node") Node node, @Name("key") String key) {
        String value = (String) node.getProperty(key, null);
        try {
            return Stream.of(new ObjectResult(JsonUtil.OBJECT_MAPPER.readValue(value, Object.class)));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure
    @Description("apoc.convert.fromJsonMap('{\"a\":42,\"b\":\"foo\",\"c\":[1,2,3]}')")
    public Stream<MapResult> fromJsonMap(@Name("map") String value) {
        try {
            return Stream.of(new MapResult(JsonUtil.OBJECT_MAPPER.readValue(value, Map.class)));
        } catch (IOException e) {
            throw new RuntimeException("Can't deserialize to Map:\n" + value, e);
        }
    }

    @Procedure
    @Description("apoc.convert.fromJsonList('[1,2,3]')")
    public Stream<ListResult> fromJsonList(@Name("list") String value) {
        try {
            return Stream.of(new ListResult(JsonUtil.OBJECT_MAPPER.readValue(value, List.class)));
        } catch (IOException e) {
            throw new RuntimeException("Can't deserialize to List:\n" + value, e);
        }
    }

    @Procedure("apoc.convert.toTree")
    @Description("apoc.convert.toTree([paths]) creates a stream of nested documents representing the at least one root of these paths")
    // todo optinally provide root node
    public Stream<MapResult> toTree(@Name("paths") List<Path> paths) {
        Map<Long, Map<String, Object>> maps = new HashMap<>(paths.size() * 100);
        for (Path path : paths) {
            Iterator<PropertyContainer> it = path.iterator();
            while (it.hasNext()) {
                Node n = (Node) it.next();
                Map<String, Object> nMap = maps.computeIfAbsent(n.getId(), (id) -> toMap(n));
                if (it.hasNext()) {
                    Relationship r = (Relationship) it.next();
                    Node m = r.getOtherNode(n);
                    Map<String, Object> mMap = maps.computeIfAbsent(m.getId(), (id) -> toMap(m));
                    String typeName = r.getType().name().toLowerCase();
                    mMap = augmentRelProperties(mMap, typeName, r);
                    // todo take direction into account and create collection into outgoing direction ??
                    // parent-[:HAS_CHILD]->(child) vs. (parent)<-[:PARENT_OF]-(child)
                    if (!nMap.containsKey(typeName)) nMap.put(typeName, new ArrayList<>(16));
                    List list = (List) nMap.get(typeName);
                    if (!list.contains(mMap))
                        list.add(mMap); // todo performance, use set instead and convert to map at the end?
                }
            }
        }
        return paths.stream()
                .map(Path::startNode)
                .map(n -> maps.remove(n.getId()))
                .filter(m -> m != null)
                .map(MapResult::new);
    }

    private Map<String, Object> augmentRelProperties(Map<String, Object> mMap, String typeName, Relationship r) {
        Map<String, Object> rProps = r.getAllProperties();
        if (rProps.isEmpty()) return mMap;
        String prefix = typeName + ".";
        Map<String, Object> result = new LinkedHashMap<>(mMap);
        rProps.forEach((k, v) -> result.put(prefix + k, v));
        return result;
    }

    private Map<String, Object> toMap(Node n) {
        Map<String, Object> props = n.getAllProperties();
        Map<String, Object> result = new LinkedHashMap<>(props.size() + 2);
        result.put("_id", n.getId());
        result.put("_type", Util.labelString(n));
        result.putAll(props);
        return result;
    }
}
