package apoc;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import java.time.Duration;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.*;

@ServiceProvider
public class ApocSettings implements SettingsDeclaration {

    public static final Setting<Boolean> apoc_export_file_enabled = newBuilder( "apoc.export.file.enabled", BOOL, false ).build();

    public static final Setting<Boolean> apoc_import_file_enabled = newBuilder( "apoc.import.file.enabled", BOOL, false ).build();

    public static final Setting<Boolean> apoc_import_file_use__neo4j__config = newBuilder( "apoc.import.file.use_neo4j_config", BOOL, false ).build();

    @Description("how often does TTL expiry check run in background")
    public static final Setting<Duration> apoc_ttl_schedule = newBuilder( "apoc.ttl.schedule", DURATION, Duration.ofMinutes(1) ).build();

    @Description("switches TTL feature on or off")
    public static final Setting<Boolean> apoc_ttl_enabled = newBuilder( "apoc.ttl.enabled", BOOL, false ).build();

    @Description("maximum number of nodes to be deleted during one iteration")
    public static final Setting<Long> apoc_ttl_limit = newBuilder( "apoc.ttl.limit", LONG, 1000l ).build();

    public static final Setting<Boolean> apoc_trigger_enabled = newBuilder( "apoc.trigger.enabled", BOOL, false ).build();

    public static final Setting<Boolean> apoc_uuid_enabled = newBuilder( "apoc.uuid.enabled", BOOL, false ).build();


    public static final Setting<String> apoc_json_zip_url = newBuilder( "apoc.json.zip.url", STRING, null ).build();

    public static final Setting<String> apoc_json_simpleJson_url = newBuilder( "apoc.json.simpleJson.url", STRING, null ).build();

    public static <T> Setting<T> dynamic(String name, SettingValueParser<T> parser) {
        return newBuilder(name, parser, null).build();
    }
}
