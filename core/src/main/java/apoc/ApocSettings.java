package apoc;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import java.time.Duration;

import static apoc.ApocConfig.*;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.*;

/**
 * NOTE: please do not amend anything to this file. It's just there for legacy - to enable reading config options from neo4j.conf.
 *
 * since 4.0 all apoc config should reside in apoc.conf file.
 */
@ServiceProvider
public class ApocSettings implements SettingsDeclaration {

    public static final Setting<Boolean> apoc_export_file_enabled = newBuilder( APOC_EXPORT_FILE_ENABLED, BOOL, false ).build();

    public static final Setting<Boolean> apoc_import_file_enabled = newBuilder( APOC_IMPORT_FILE_ENABLED, BOOL, false ).build();

    public static final Setting<Boolean> apoc_import_file_use__neo4j__config = newBuilder( APOC_IMPORT_FILE_USE_NEO4J_CONFIG, BOOL, true ).build();

    @Description("how often does TTL expiry check run in background")
    public static final Setting<Duration> apoc_ttl_schedule = newBuilder(APOC_TTL_SCHEDULE, DURATION, Duration.ofMinutes(1) ).build();

    @Description("switches TTL feature on or off")
    public static final Setting<Boolean> apoc_ttl_enabled = newBuilder(APOC_TTL_ENABLED, BOOL, false ).build();

    @Description("maximum number of nodes to be deleted during one iteration")
    public static final Setting<Long> apoc_ttl_limit = newBuilder(APOC_TTL_LIMIT, LONG, 1000L ).build();

    public static final Setting<Boolean> apoc_trigger_enabled = newBuilder(APOC_TRIGGER_ENABLED, BOOL, false ).build();

    public static final Setting<Boolean> apoc_uuid_enabled = newBuilder(APOC_UUID_ENABLED, BOOL, false ).build();

    @Deprecated
    public static final Setting<String> apoc_initializer_cypher = newBuilder(APOC_CONFIG_INITIALIZER_CYPHER, STRING, null).build();

    public static final Setting<Long> apoc_jobs_queue_size = newBuilder(APOC_CONFIG_JOBS_QUEUE_SIZE, LONG, null).build();

    public static final Setting<Long> apoc_jobs_pool_num_threads = newBuilder(APOC_CONFIG_JOBS_POOL_NUM_THREADS, LONG, null).build();

    public static final Setting<Long> apoc_jobs_scheduled_num_threads = newBuilder(APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS, LONG, null).build();

//    public static final Setting<String> apoc_json_zip_url = newBuilder(APOC_JSON_ZIP_URL, STRING, null ).build();

//    public static final Setting<String> apoc_json_simpleJson_url = newBuilder(APOC_JSON_SIMPLE_JSON_URL, STRING, null ).build();

    public static <T> Setting<T> dynamic(String name, SettingValueParser<T> parser) {
        return newBuilder(name, parser, null).build();
    }
}
