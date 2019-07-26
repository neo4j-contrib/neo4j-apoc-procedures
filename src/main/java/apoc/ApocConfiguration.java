package apoc;

import apoc.cache.Static;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;
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

    private static final Map<String, Object> DEFAULTS = Util.map("import.file.use_neo4j_config", true);

    static {
        PARAM_WHITELIST.put("dbms.security.allow_csv_import_from_file_urls", "import.file.allow_read_from_filesystem");

        // Contains list of all dbms.directories.* settings supported by Neo4j.
        for(String directorySetting : FileUtils.NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES) {
            PARAM_WHITELIST.put(directorySetting, directorySetting);
        }
    }

    public static void initialize(GraphDatabaseAPI db) {
        Static.clear();
        Config neo4jConfig = db.getDependencyResolver().resolveDependency(Config.class);

        Map<String, Setting<Object>> params = neo4jConfig.getDeclaredSettings();
        apocConfig.clear();
        apocConfig.putAll(Util.subMap(params, PREFIX));
        mergeDefaults();
        PARAM_WHITELIST.forEach((k, v) -> {
            Setting<Object> setting = neo4jConfig.getSetting(k);
            Object configValue = neo4jConfig.get(setting);
            if (configValue!=null) {
                apocConfig.put(v, configValue.toString());
            }
        });
        config.clear();
        params.forEach((k, v) -> { if (!SKIP.matcher(k).find()) {config.put(k, v);} });
    }

    private static void mergeDefaults() {
        DEFAULTS.forEach((key, value) -> apocConfig.computeIfAbsent(key, (k) -> value));
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
