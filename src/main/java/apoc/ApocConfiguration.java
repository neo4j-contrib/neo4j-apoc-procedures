package apoc;

import apoc.cache.Static;
import apoc.util.Util;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;

/**
 * @author mh
 * @since 19.05.16
 */
public class ApocConfiguration {
    public static final String PREFIX = "apoc.";
    private static Map<String, Object> config = new HashMap<>(10);

    public static void initialize(GraphDatabaseAPI db) {
        Static.clear();
        Map<String, String> params = db.getDependencyResolver().resolveDependency(Config.class).getParams();
        config.clear();
        config.putAll(Util.subMap(params, PREFIX));
    }

    public static Map<String, Object> get(String prefix) {
        return Util.subMap(config, prefix);
    }

    public static <T> T get(String key, T defaultValue) {
        return (T) config.getOrDefault(key, defaultValue);
    }

    public static void addToConfig(Map<String,Object> newConfig) {
        config.putAll(newConfig);
    }
}
