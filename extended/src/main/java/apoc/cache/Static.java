package apoc.cache;

import apoc.Extended;
import apoc.ExtendedApocConfig;
import apoc.result.KeyValueResult;
import apoc.result.ObjectResultExtended;
import apoc.util.UtilExtended;
import apoc.util.collection.IteratorsExtended;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 22.05.16
 */
@Extended
public class Static {

    @Context
    public GraphDatabaseAPI db;
    
    @Context
    public ExtendedApocConfig extendedApocConfig;

    private static Map<String,Object> storage = new HashMap<>();

    @UserFunction("apoc.static.get")
    @Description("apoc.static.get(name) - returns statically stored value from config (apoc.static.<key>) or server lifetime storage")
    public Object get(@Name("key") String key) {
        return storage.getOrDefault(key, fromConfig(key));
    }

    @UserFunction("apoc.static.getAll")
    @Description("apoc.static.getAll(prefix) - returns statically stored values from config (apoc.static.<prefix>.*) or server lifetime storage")
    public Map<String,Object> getAll(@Name("prefix") String prefix) {
        return getFromConfigAndStorage(prefix);
    }

    private HashMap<String, Object> getFromConfigAndStorage(@Name("prefix") String prefix) {

        HashMap<String, Object> result = new HashMap<>();
        String configPrefix = prefix.isEmpty() ? "apoc.static": "apoc.static." + prefix;
        IteratorsExtended.stream(extendedApocConfig.getKeys(configPrefix)).forEach(s -> result.put(s.substring(configPrefix.length()+1), extendedApocConfig.getString(s)));
        result.putAll(UtilExtended.subMap(storage, prefix));
        return result;
    }

    @Procedure("apoc.static.list")
    @Description("apoc.static.list(prefix) - returns statically stored values from config (apoc.static.<prefix>.*) or server lifetime storage")
    public Stream<KeyValueResult> list(@Name("prefix") String prefix) {
        HashMap<String, Object> result = getFromConfigAndStorage(prefix);
        return result.entrySet().stream().map( e -> new KeyValueResult(e.getKey(),e.getValue()));
    }

    private Object fromConfig(@Name("key") String key) {
        return extendedApocConfig.getString("apoc.static."+key);
    }

    @Procedure("apoc.static.set")
    @Description("apoc.static.set(name, value) - stores value under key for server lifetime storage, returns previously stored or configured value")
    public Stream<ObjectResultExtended> set(@Name("key") String key, @Name("value") Object value) {
        Object previous = value == null ? storage.remove(key) : storage.put(key, value);
        return Stream.of(new ObjectResultExtended(previous==null ? fromConfig(key) : previous));
    }

    public static void clear() {
        storage.clear();
    }
}
