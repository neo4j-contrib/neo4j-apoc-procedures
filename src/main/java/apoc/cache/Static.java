package apoc.cache;

import apoc.ApocConfiguration;
import apoc.Description;
import apoc.result.KeyValueResult;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.util.Util;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 22.05.16
 */
public class Static {

    @Context
    public GraphDatabaseAPI db;

    private static Map<String,Object> storage = new HashMap<>();

    @Procedure("apoc.static.get")
    @Description("apoc.static.get(name) - returns statically stored value from config (apoc.static.<key>) or server lifetime storage")
    public Stream<ObjectResult> get(@Name("key") String key) {
        return Stream.of(new ObjectResult(storage.getOrDefault(key, fromConfig(key))));
    }

    @Procedure("apoc.static.getAll")
    @Description("apoc.static.getAll(prefix) - returns statically stored values from config (apoc.static.<prefix>.*) or server lifetime storage")
    public Stream<MapResult> getAll(@Name("prefix") String prefix) {
        return Stream.of(new MapResult(getFromConfigAndStorage(prefix)));
    }

    private HashMap<String, Object> getFromConfigAndStorage(@Name("prefix") String prefix) {
        Map<String,Object> config = ApocConfiguration.get("static." + prefix);
        HashMap<String, Object> result = new HashMap<>(config);
        result.putAll(Util.subMap(storage, prefix));
        return result;
    }

    @Procedure("apoc.static.list")
    @Description("apoc.static.list(prefix) - returns statically stored values from config (apoc.static.<prefix>.*) or server lifetime storage")
    public Stream<KeyValueResult> list(@Name("prefix") String prefix) {
        HashMap<String, Object> result = getFromConfigAndStorage(prefix);
        return result.entrySet().stream().map( e -> new KeyValueResult(e.getKey(),e.getValue()));
    }

    private Object fromConfig(@Name("key") String key) {
        return ApocConfiguration.get("static."+key,null);
    }

    @Procedure("apoc.static.set")
    @Description("apoc.static.set(name, value) - stores value under key for server livetime storage, returns previously stored or configured value")
    public Stream<ObjectResult> set(@Name("key") String key, @Name("value") Object value) {
        Object previous = value == null ? storage.remove(key) : storage.put(key, value);
        return Stream.of(new ObjectResult(previous==null ? fromConfig(key) : previous));
    }

    public static void clear() {
        storage.clear();
    }
}
