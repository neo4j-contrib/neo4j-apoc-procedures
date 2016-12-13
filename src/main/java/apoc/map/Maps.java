package apoc.map;

import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.*;

public class Maps {

    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("apoc.map.groupBy([maps/nodes/relationships],'key') yield value - creates a map of the list keyed by the given property, with single values")
    public Map<String,Object> groupBy(@Name("values") List<Object> values, @Name("key") String key) {
        Map<String,Object> result = new LinkedHashMap<>(values.size());
        for (Object value : values) {
            Object id = getKey(key, value);
            if (id != null) result.put(id.toString(), value);
        }
        return result;
    }
    @UserFunction
    @Description("apoc.map.groupByMulti([maps/nodes/relationships],'key') yield value - creates a map of the list keyed by the given property, with list values")
    public Map<String,List<Object>> groupByMulti(@Name("values") List<Object> values, @Name("key") String key) {
        Map<String,List<Object>> result = new LinkedHashMap<>(values.size());
        for (Object value : values) {
            Object id = getKey(key, value);
            if (id != null) result.compute(id.toString(), (k,list) -> {
                if (list==null) list = new ArrayList<>();
                list.add(value);
                return list;
            });
        }
        return result;
    }

    public Object getKey(@Name("key") String key, Object value) {
        Object id = null;
        if (value instanceof Map) {
            id = ((Map)value).get(key);
        }
        if (value instanceof PropertyContainer) {
            id = ((PropertyContainer)value).getProperty(key,null);
        }
        return id;
    }

    @UserFunction
    @Description("apoc.map.fromNodes(label, property)")
    public Map<String, Node> fromNodes(@Name("label") String label, @Name("property") String property) {
        Map<String, Node> result = new LinkedHashMap<>(10000);
        try (ResourceIterator<Node> nodes = db.findNodes(Label.label(label))) {
            while (nodes.hasNext()) {
                Node node = nodes.next();
                Object key = node.getProperty(property, null);
                if (key!=null) {
                    result.put(key.toString(), node);
                }
            }
        }
        return result;
    }

    @UserFunction
    @Description("apoc.map.fromPairs([[key,value],[key2,value2],...])")
    public Map<String,Object> fromPairs(@Name("pairs") List<List<Object>> pairs) {
        return Util.mapFromPairs(pairs);
    }

    @UserFunction
    @Description("apoc.map.fromLists([keys],[values])")
    public Map<String,Object> fromLists(@Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return Util.mapFromLists(keys, values);
    }
    @UserFunction
    @Description("apoc.map.fromValues([key1,value1,key2,value2,...])")
    public Map<String,Object> fromValues(@Name("values") List<Object> values) {
        return Util.map(values);
    }

    @UserFunction
    @Description("apoc.map.merge(first,second) - merges two maps")
    public Map<String,Object> merge(@Name("first") Map<String,Object> first, @Name("second") Map<String,Object> second) {
        return Util.merge(first,second);
    }

    @UserFunction
    @Description("apoc.map.mergeList([{maps}]) yield value - merges all maps in the list into one")
    public Map<String,Object> mergeList(@Name("maps") List<Map<String,Object>> maps) {
        Map<String,Object> result = new LinkedHashMap<>(maps.size());
        for (Map<String, Object> map : maps) {
            result.putAll(map);
        }
        return result;
    }

    @UserFunction
    @Description("apoc.map.setKey(map,key,value)")
    public Map<String,Object> setKey(@Name("map") Map<String,Object> map, @Name("key") String key, @Name("value") Object value) {
        return Util.merge(map, Util.map(key,value));
    }

    @UserFunction
    @Description("apoc.map.setEntry(map,key,value)")
    public Map<String,Object> setEntry(@Name("map") Map<String,Object> map, @Name("key") String key, @Name("value") Object value) {
        return Util.merge(map, Util.map(key,value));
    }

    @UserFunction
    @Description("apoc.map.setPairs(map,[[key1,value1],[key2,value2])")
    public Map<String,Object> setPairs(@Name("map") Map<String,Object> map, @Name("pairs") List<List<Object>> pairs) {
        return Util.merge(map, Util.mapFromPairs(pairs));
    }

    @UserFunction
    @Description("apoc.map.setLists(map,[keys],[values])")
    public Map<String,Object> setLists(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return Util.merge(map, Util.mapFromLists(keys, values));
    }

    @UserFunction
    @Description("apoc.map.setValues(map,[key1,value1,key2,value2])")
    public Map<String,Object> setValues(@Name("map") Map<String,Object> map, @Name("pairs") List<Object> pairs) {
        return Util.merge(map, Util.map(pairs));
    }

    @UserFunction
    @Description("apoc.map.removeKey(map,key)")
    public Map<String,Object> removeKey(@Name("map") Map<String,Object> map, @Name("key") String key) {
        Map<String, Object> res = new LinkedHashMap<>(map);
        res.remove(key);
        return res;
    }

    @UserFunction
    @Description("apoc.map.removeKeys(map,keys)")
    public Map<String,Object> removeKeys(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys) {
        Map<String, Object> res = new LinkedHashMap<>(map);
        res.keySet().removeAll(keys);
        return res;
    }

    @UserFunction
    @Description("apoc.map.clean(map,[skip,keys],[skip,values]) yield map removes the keys and values contained in those lists, good for data cleaning from CSV/JSON")
    public Map<String,Object> clean(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        HashSet<String> keySet = new HashSet<>(keys);
        HashSet<Object> valueSet = new HashSet<>(values);

        LinkedHashMap<String, Object> res = new LinkedHashMap<>(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (keySet.contains(entry.getKey()) || value == null || valueSet.contains(value) || valueSet.contains(value.toString())) continue;
            res.put(entry.getKey(),value);
        }
        return res;
    }

    @UserFunction
    @Description("apoc.map.flatten(map) yield map - flattens nested items in map using dot notation")
    public Map<String,Object> flatten(@Name("map") Map<String, Object> map) {
        Map<String, Object> flattenedMap = new HashMap<>();
        flattenMapRecursively(flattenedMap, map, "");
        return flattenedMap;
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
