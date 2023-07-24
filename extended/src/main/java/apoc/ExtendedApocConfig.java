package apoc;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;

import apoc.util.SimpleRateLimiter;
import java.net.URL;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.combined.CombinedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.ex.ConversionException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

public class ExtendedApocConfig extends LifecycleAdapter
{
    public static final String APOC_TTL_SCHEDULE = "apoc.ttl.schedule";
    public static final String APOC_TTL_ENABLED = "apoc.ttl.enabled";
    public static final String APOC_TTL_LIMIT = "apoc.ttl.limit";
    public static final String APOC_TTL_SCHEDULE_DB = "apoc.ttl.schedule.%s";
    public static final String APOC_TTL_ENABLED_DB = "apoc.ttl.enabled.%s";
    public static final String APOC_TTL_LIMIT_DB = "apoc.ttl.limit.%s";
    public static final String APOC_UUID_ENABLED = "apoc.uuid.enabled";
    public static final String APOC_UUID_ENABLED_DB = "apoc.uuid.enabled.%s";
    public static final String APOC_UUID_FORMAT = "apoc.uuid.format";
    public static final String APOC_OPENAI_KEY = "apoc.openai.key";
    public enum UuidFormatType { hex, base64 }

    // These were earlier added via the Neo4j config using the ApocSettings.java class
    private static final Map<String,Object> configDefaultValues =
            Map.of(
                    APOC_TTL_SCHEDULE, Duration.ofMinutes(1),
                    APOC_TTL_ENABLED, false,
                    APOC_TTL_LIMIT, 1000L,
                    APOC_UUID_ENABLED, false
            );

    private final Log log;

    private Configuration config;

    private static ExtendedApocConfig theInstance;

    private ExtendedApocConfig.LoggingType loggingType;
    private SimpleRateLimiter rateLimiter;

    /**
     * keep track if this instance is already initialized so dependent class can wait if needed
     */
    private boolean initialized = false;

    private static final String DEFAULT_PATH = ".";
    public static final String CONFIG_DIR = "config-dir=";

    public ExtendedApocConfig(LogService log, GlobalProcedures globalProceduresRegistry) {
        this.log = log.getInternalLog(ApocConfig.class);
        theInstance = this;

        // expose this config instance via `@Context ApocConfig config`
        globalProceduresRegistry.registerComponent((Class<ExtendedApocConfig>) getClass(), ctx -> this, true);
        this.log.info("successfully registered ExtendedApocConfig for @Context");
    }

    @Override
    public void init() {
        log.debug("called init");
        // grab NEO4J_CONF from environment. If not set, calculate it from sun.java.command system property
        String neo4jConfFolder = System.getenv().getOrDefault("NEO4J_CONF", determineNeo4jConfFolder());
        System.setProperty("NEO4J_CONF", neo4jConfFolder);
        log.info("system property NEO4J_CONF set to %s", neo4jConfFolder);
        loadConfiguration();
        initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    protected String determineNeo4jConfFolder() {
        String command = System.getProperty(SUN_JAVA_COMMAND);
        if (command == null) {
            log.warn("system property %s is not set, assuming '.' as conf dir. This might cause `apoc.conf` not getting loaded.", SUN_JAVA_COMMAND);
            return DEFAULT_PATH;
        } else {
            final String neo4jConfFolder = Stream.of(command.split("--"))
                    .map(String::trim)
                    .filter(s -> s.startsWith(CONFIG_DIR))
                    .map(s -> s.substring(CONFIG_DIR.length()))
                    .findFirst()
                    .orElse(DEFAULT_PATH);
            if (DEFAULT_PATH.equals(neo4jConfFolder)) {
                log.info("cannot determine conf folder from sys property %s, assuming '.' ", command);
            } else {
                log.info("from system properties: NEO4J_CONF=%s", neo4jConfFolder);
            }
            return neo4jConfFolder;
        }
    }

    /**
     * use apache commons to load configuration
     * classpath:/apoc-config.xml contains a description where to load configuration from
     */

    protected void loadConfiguration() {
        try {

            URL resource = getClass().getClassLoader().getResource("apoc-config.xml");
            log.info("loading apoc meta config from %s", resource.toString());
            CombinedConfigurationBuilder builder = new CombinedConfigurationBuilder()
                    .configure(new Parameters().fileBased().setURL(resource));
            config = builder.getConfiguration();

            // set config settings not explicitly set in apoc.conf to their default value
            configDefaultValues.forEach((k,v) -> {
                if (!config.containsKey(k))
                {
                   config.setProperty(k, v);
                   log.info("setting APOC config to default value: " + k + "=" + v);
                }
            });

            initLogging();
        } catch ( ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    protected Configuration getConfig() {
        return config;
    }

    public ExtendedApocConfig.LoggingType getLoggingType() {
        return loggingType;
    }

    public SimpleRateLimiter getRateLimiter() {
        return rateLimiter;
    }

    public void setLoggingType( ExtendedApocConfig.LoggingType loggingType) {
        this.loggingType = loggingType;
    }

    public void setRateLimiter(SimpleRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    public enum LoggingType {none, safe, raw}

    private void initLogging() {
        loggingType = ExtendedApocConfig.LoggingType.valueOf(config.getString("apoc.user.log.type", "safe").trim());
        rateLimiter = new SimpleRateLimiter(getInt( "apoc.user.log.window.time", 10000), getInt("apoc.user.log.window.ops", 10));
    }

    public static ExtendedApocConfig extendedApocConfig() {
        return theInstance;
    }

    /*
     * delegate methods for Configuration
     */

    public Iterator<String> getKeys(String prefix) {
        return config.getKeys(prefix);
    }

    public boolean containsKey(String key) {
        return config.containsKey(key);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return getConfig().getBoolean(key, defaultValue);
    }
    public String getString(String key, String defaultValue) {
        return getConfig().getString(key, defaultValue);
    }

    public <T extends Enum<T>> T getEnumProperty(String key, Class<T> cls, T defaultValue) {
        var value = config.getString(key, defaultValue.toString()).trim();
        try {
            return T.valueOf(cls, value);
        } catch (IllegalArgumentException e) {
            log.error("Wrong value '{}' for parameter '{}' is provided. Default value is used: '{}'", value, key, defaultValue);
            return defaultValue;
        }
    }

    private int getInt(String key, int defaultValue) {
        try {
            return config.getInt(key, defaultValue);
        } catch ( ConversionException e) {
            Object o = config.getProperty(key);
            if (o instanceof Duration ) {
                return (int) ((Duration)o).getSeconds();
            } else {
                throw new IllegalArgumentException("don't know how to convert for config option " + key, e);
            }
        }
    }
}
