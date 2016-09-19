package apoc.map;

import org.neo4j.procedure.Description;
import apoc.result.MapResult;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

public class Maps {

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.map.fromPairs([[key,value],[key2,value2],...])")
    public Stream<MapResult> fromPairs(@Name("pairs") List<List<Object>> pairs) {
        return Stream.of(new MapResult(Util.mapFromPairs(pairs)));
    }

    @Procedure
    @Description("apoc.map.fromLists([keys],[values])")
    public Stream<MapResult> fromLists(@Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return Stream.of(new MapResult(Util.mapFromLists(keys, values)));
    }
    @Procedure
    @Description("apoc.map.fromValues([key1,value1,key2,value2,...])")
    public Stream<MapResult> fromValues(@Name("values") List<Object> values) {
        return Stream.of(new MapResult(Util.map(values)));
    }

    @Procedure
    @Description("apoc.map.merge(first,second) yield value - merges two maps")
    public Stream<MapResult> merge(@Name("first") Map<String,Object> first, @Name("second") Map<String,Object> second) {
        return Stream.of(new MapResult(Util.merge(first,second)));
    }

    @Procedure
    @Description("apoc.map.setKey(map,key,value)")
    public Stream<MapResult> setKey(@Name("map") Map<String,Object> map, @Name("key") String key, @Name("value") Object value) {
        return Stream.of(new MapResult(Util.merge(map, Util.map(key,value))));
    }

    @Procedure
    @Description("apoc.map.setEntry(map,key,value)")
    public Stream<MapResult> setEntry(@Name("map") Map<String,Object> map, @Name("key") String key, @Name("value") Object value) {
        return Stream.of(new MapResult(Util.merge(map, Util.map(key,value))));
    }

    @Procedure
    @Description("apoc.map.setPairs(map,[[key1,value1],[key2,value2])")
    public Stream<MapResult> setPairs(@Name("map") Map<String,Object> map, @Name("pairs") List<List<Object>> pairs) {
        return Stream.of(new MapResult(Util.merge(map, Util.mapFromPairs(pairs))));
    }

    @Procedure
    @Description("apoc.map.setLists(map,[keys],[values])")
    public Stream<MapResult> setLists(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return Stream.of(new MapResult(Util.merge(map, Util.mapFromLists(keys, values))));
    }

    @Procedure
    @Description("apoc.map.setValues(map,[key1,value1,key2,value2])")
    public Stream<MapResult> setValues(@Name("map") Map<String,Object> map, @Name("pairs") List<Object> pairs) {
        return Stream.of(new MapResult(Util.merge(map, Util.map(pairs))));
    }

    @Procedure
    @Description("apoc.map.removeKey(map,key)")
    public Stream<MapResult> removeKey(@Name("map") Map<String,Object> map, @Name("key") String key) {
        Map<String, Object> res = new LinkedHashMap<>(map);
        res.remove(key);
        return Stream.of(new MapResult(res));
    }

    @Procedure
    @Description("apoc.map.removeKeys(map,keys)")
    public Stream<MapResult> removeKeys(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys) {
        Map<String, Object> res = new LinkedHashMap<>(map);
        res.keySet().removeAll(keys);
        return Stream.of(new MapResult(res));
    }

    @Procedure
    @Description("apoc.map.clean(map,[skip,keys],[skip,values]) yield map removes the keys and values contained in those lists, good for data cleaning from CSV/JSON")
    public Stream<MapResult> clean(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        HashSet<String> keySet = new HashSet<>(keys);
        HashSet<Object> valueSet = new HashSet<>(values);

        LinkedHashMap<String, Object> res = new LinkedHashMap<>(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (keySet.contains(entry.getKey()) || value == null || valueSet.contains(value) || valueSet.contains(value.toString())) continue;
            res.put(entry.getKey(),value);
        }
        return Stream.of(new MapResult(res));
    }

    @Procedure
    @Description("apoc.map.flatten(map) yield map - flattens nested items in map using dot notation")
    public Stream<MapResult> flatten(@Name("map") Map<String, Object> map) {
        Map<String, Object> flattenedMap = new HashMap<>();
        flattenMapRecursively(flattenedMap, map, "");
        return Stream.of(new MapResult(flattenedMap));
    }

    @SuppressWarnings("unchecked")
    private void flattenMapRecursively(Map<String, Object> flattenedMap, Map<String, Object> map, String prefix) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
             if (entry.getValue() instanceof Map) {
                 flattenMapRecursively(flattenedMap, (Map<String, Object>) entry.getValue(), prefix + entry.getKey() + ".");
             } else {
                 flattenedMap.put(prefix + entry.getKey(), entry.getValue());
             }
        }
    }

}
