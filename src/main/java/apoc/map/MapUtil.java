package apoc.map;

import apoc.result.MapResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

public class MapUtil {

    @Context
    public GraphDatabaseService db;

    @Procedure
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
    public Stream<MapResult> fromLists(@Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return Stream.of(new MapResult(mapFromLists(keys, values)));
    }

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

    public Stream<MapResult> setKey(@Name("map") Map<String,Object> map, @Name("key") String key, @Name("value") Object value) {
        LinkedHashMap<String, Object> res = new LinkedHashMap<>(map);
        res.put(key,value);
        return Stream.of(new MapResult(res));
    }
}
