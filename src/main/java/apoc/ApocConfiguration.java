package apoc;

import apoc.cache.Static;
import apoc.util.Util;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author mh
 * @since 19.05.16
 */
public class ApocConfiguration {
    public static final String PREFIX = "apoc.";
    private static final Pattern SKIP = Pattern.compile("(ur[il]|pass|cred)",Pattern.CASE_INSENSITIVE);
    private static Map<String, Object> apocConfig = new HashMap<>(10);
    private static Map<String, Object> config = new HashMap<>(32);
    private static final Map<String, String> PARAM_WHITELIST = new HashMap<>(2);

    static {
        PARAM_WHITELIST.put("dbms.directories.import", "import.file.directory");
        PARAM_WHITELIST.put("dbms.security.allow_csv_import_from_file_urls", "import.file.allow_read_from_filesystem");
    }

    public static void initialize(GraphDatabaseAPI db) {
        Static.clear();
        Map<String, String> params = db.getDependencyResolver().resolveDependency(Config.class).getParams();
        apocConfig.clear();
        apocConfig.putAll(Util.subMap(params, PREFIX));
        PARAM_WHITELIST.forEach((k, v) -> apocConfig.put(v, params.get(k)) );
        config.clear();
        params.forEach((k, v) -> { if (!SKIP.matcher(k).find()) {config.put(k, v);} });
    }

    public static Map<String, Object> get(String prefix) {
        return Util.subMap(apocConfig, prefix);
    }

    public static <T> T get(String key, T defaultValue) {
        return (T) apocConfig.getOrDefault(key, defaultValue);
    }
    public static boolean isEnabled(String key) {
        return Util.toBoolean(apocConfig.getOrDefault(key, false));
    }

    public static void addToConfig(Map<String,Object> newConfig) {
        apocConfig.putAll(newConfig);
    }

    public static Map<String,Object> list() {
        return config;
    }
}
