package apoc.config;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.MapResult;
import apoc.util.collection.Iterators;
import org.apache.commons.configuration2.Configuration;
import org.neo4j.common.DependencyResolver;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_CONFIG_JOBS_POOL_NUM_THREADS;
import static apoc.ApocConfig.APOC_CONFIG_JOBS_QUEUE_SIZE;
import static apoc.ApocConfig.APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS;
import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ExtendedApocConfig.APOC_TTL_ENABLED;
import static apoc.ExtendedApocConfig.APOC_TTL_LIMIT;
import static apoc.ExtendedApocConfig.APOC_TTL_SCHEDULE;
import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED;
import static apoc.ExtendedApocConfig.APOC_UUID_FORMAT;
import static apoc.custom.CypherProceduresHandler.CUSTOM_PROCEDURES_REFRESH;

/**
 * @author mh
 * @since 28.10.16
 */
@Extended
public class Config {
    
    // some config keys are hard-coded because belong to `core`, which is no longer accessed from `extended`
    private static final Set<String> WHITELIST_CONFIGS = Set.of(
            // apoc.import.
            APOC_IMPORT_FILE_ENABLED,
            APOC_IMPORT_FILE_USE_NEO4J_CONFIG,
            APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM,

            // apoc.export.
            APOC_EXPORT_FILE_ENABLED,
            
            // apoc.trigger.
            APOC_TRIGGER_ENABLED,
            "apoc.trigger.refresh",

            // apoc.uuid.
            APOC_UUID_ENABLED,
            APOC_UUID_FORMAT,
            
            // apoc.ttl.
            APOC_TTL_SCHEDULE,
            APOC_TTL_ENABLED,
            APOC_TTL_LIMIT,
            
            // apoc.jobs.
            APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS,
            APOC_CONFIG_JOBS_POOL_NUM_THREADS,
            APOC_CONFIG_JOBS_QUEUE_SIZE,
            
            // apoc.http.
            "apoc.http.timeout.connect",
            "apoc.http.timeout.read",
            
            // apoc.custom.
            CUSTOM_PROCEDURES_REFRESH,

            // apoc.spatial. - other configs can have sensitive credentials
            "apoc.spatial.geocode.osm.throttle",
            "apoc.spatial.geocode.google.throttle"
    );
    
    public static class ConfigResult {
        public final String key;
        public final Object value;

        public ConfigResult(String key, Object value) {
            this.key = key;
            this.value = value;
        }
    }

    @Context
    public DependencyResolver dependencyResolver;

    @Admin
    @Description("apoc.config.list | Lists the Neo4j configuration as key,value table")
    @Procedure
    public Stream<ConfigResult> list() {
        Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();
        return getApocConfigs(config)
                .map(s -> new ConfigResult(s, config.getString(s)));
    }

    @Admin
    @Description("apoc.config.map | Lists the Neo4j configuration as map")
    @Procedure
    public Stream<MapResult> map() {
        Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();
        Map<String, Object> configMap = getApocConfigs(config)
                .collect(Collectors.toMap(s -> s, s -> config.getString(s)));
        return Stream.of(new MapResult(configMap));
    }

    private static Stream<String> getApocConfigs(Configuration config) {
        // we use startsWith(..) because we can have e.g. a config `apoc.uuid.enabled.<dbName>`
        return Iterators.stream(config.getKeys())
                .filter(conf -> WHITELIST_CONFIGS.stream().anyMatch(conf::startsWith));
    }
}
