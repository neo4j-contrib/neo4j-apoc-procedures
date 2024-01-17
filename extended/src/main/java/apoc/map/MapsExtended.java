package apoc.map;

import apoc.Extended;
import apoc.util.Util;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Extended
public class MapsExtended {

    @UserFunction("apoc.map.renameKey")
    @Description("Rename the given key(s) in the `MAP`.")
    public Map<String, Object> renameKeyRecursively(@Name("map") Map<String, Object> map,
                                                    @Name("keyFrom") String keyFrom,
                                                    @Name("keyTo") String keyTo,
                                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        boolean recursive = Util.toBoolean(config.getOrDefault("recursive", true));
        if (recursive) {
            return (Map<String, Object>) renameKeyRecursively(map, keyFrom, keyTo);
        }
        if (map.containsKey(keyFrom)) {
            Object value = map.remove(keyFrom);
            map.put(keyTo, value);
        }
        return map;
    }

    private Object renameKeyRecursively(Object object, String keyFrom, String keyTo) {
        if (object instanceof Map<?, ?>) {
            return ((Map<String, Object>) object).entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> {
                                String key = e.getKey();
                                return key.equals(keyFrom) ? keyTo : key;
                            },
                            e -> renameKeyRecursively(e.getValue(), keyFrom, keyTo))
                    );
        }
        if (object instanceof List<?> subList) {
            return subList.stream()
                    .map(v -> renameKeyRecursively(v, keyFrom, keyTo))
                    .toList();
        }
        return object;
    }

}
