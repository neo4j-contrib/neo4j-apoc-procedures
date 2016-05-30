package apoc.map;

import apoc.Description;
import apoc.result.MapResult;
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
        return Stream.of(new MapResult(mapFromPairs(pairs)));
    }

    private Map<String, Object> mapFromPairs(@Name("pairs") List<List<Object>> pairs) {
        if (pairs.isEmpty()) return Collections.<String,Object>emptyMap();
        Map<String,Object> map = new LinkedHashMap<>(pairs.size());
        for (List<Object> pair : pairs) {
            if (pair.isEmpty()) continue;
            Object key = pair.get(0);
            if (key==null) continue;
            Object value = pair.size() >= 2 ? pair.get(1) : null;
            map.put(key.toString(),value);
        }
        return map;
    }

    @Procedure
    @Description("apoc.map.fromLists([keys],[values])")
    public Stream<MapResult> fromLists(@Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return Stream.of(new MapResult(mapFromLists(keys, values)));
    }

    // TODO    @Description("apoc.map.fromValues([key,value,key1,value1])")

    private Map<String, Object> mapFromLists(@Name("keys") List<String> keys, @Name("values") List<Object> values) {
        assert keys.size() == values.size();
        if (keys.isEmpty()) return Collections.<String,Object>emptyMap();
        if (keys.size()==1) return Collections.singletonMap(keys.get(0),values.get(0));
        ListIterator<Object> it = values.listIterator();
        Map<String, Object> res = new LinkedHashMap<>(keys.size());
        for (String key : keys) {
            res.put(key,it.next());
        }
        return res;
    }

    @Procedure
    @Description("apoc.map.setKey(map,key,value)")
    public Stream<MapResult> setKey(@Name("map") Map<String,Object> map, @Name("key") String key, @Name("value") Object value) {
        Map<String, Object> res = new LinkedHashMap<>(map);
        res.put(key,value);
        return Stream.of(new MapResult(res));
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
