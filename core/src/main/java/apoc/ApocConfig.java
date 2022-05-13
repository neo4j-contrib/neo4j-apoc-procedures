package apoc;

import apoc.export.util.ExportConfig;
import apoc.util.SimpleRateLimiter;
import inet.ipaddr.IPAddressString;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.combined.CombinedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.ex.ConversionException;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.LogService;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static apoc.util.FileUtils.isFile;
import static org.neo4j.configuration.BootloaderSettings.lib_directory;
import static org.neo4j.configuration.BootloaderSettings.run_directory;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.logical_logs_location;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.load_csv_file_url_root;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

public class ApocConfig extends LifecycleAdapter {
    public static final String SUN_JAVA_COMMAND = "sun.java.command";
    public static final String CYPHER_IP_BLOCKLIST = "unsupported.dbms.cypher_ip_blocklist";
    public static final String APOC_IMPORT_FILE_ENABLED = "apoc.import.file.enabled";
    public static final String APOC_EXPORT_FILE_ENABLED = "apoc.export.file.enabled";
    public static final String APOC_IMPORT_FILE_USE_NEO4J_CONFIG = "apoc.import.file.use_neo4j_config";
    public static final String APOC_TTL_SCHEDULE = "apoc.ttl.schedule";
    public static final String APOC_TTL_ENABLED = "apoc.ttl.enabled";
    public static final String APOC_TTL_LIMIT = "apoc.ttl.limit";
    public static final String APOC_TTL_SCHEDULE_DB = "apoc.ttl.schedule.%s";
    public static final String APOC_TTL_ENABLED_DB = "apoc.ttl.enabled.%s";
    public static final String APOC_TTL_LIMIT_DB = "apoc.ttl.limit.%s";
    public static final String APOC_TRIGGER_ENABLED = "apoc.trigger.enabled";
    public static final String APOC_UUID_ENABLED = "apoc.uuid.enabled";
    public static final String APOC_UUID_ENABLED_DB = "apoc.uuid.enabled.%s";
    public static final String APOC_UUID_FORMAT = "apoc.uuid.format";
    public enum UuidFormatType { hex, base64 }
    public static final String APOC_JSON_ZIP_URL = "apoc.json.zip.url";  // TODO: check if really needed
    public static final String APOC_JSON_SIMPLE_JSON_URL = "apoc.json.simpleJson.url"; // TODO: check if really needed
    public static final String APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM = "apoc.import.file.allow_read_from_filesystem";
    public static final String APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS = "apoc.jobs.scheduled.num_threads";
    public static final String APOC_CONFIG_JOBS_POOL_NUM_THREADS = "apoc.jobs.pool.num_threads";
    public static final String APOC_CONFIG_JOBS_QUEUE_SIZE = "apoc.jobs.queue.size";
    public static final String APOC_CONFIG_INITIALIZER = "apoc.initializer";
    public static final String LOAD_FROM_FILE_ERROR = "Import from files not enabled, please set apoc.import.file.enabled=true in your apoc.conf";

    /**
     * @deprecated
     * This has been replaced by database-specific initialisers.
     * Use apoc.initializer.<database name> instead.
     */
    @Deprecated
    public static final String APOC_CONFIG_INITIALIZER_CYPHER = APOC_CONFIG_INITIALIZER + ".cypher";

    private static final List<Setting> NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES = new ArrayList<>(Arrays.asList(
            data_directory,
            load_csv_file_url_root,
            logs_directory,
            plugin_dir,
            logical_logs_location,
            transaction_logs_root_path,
            run_directory,
            lib_directory,
            neo4j_home
    ));
    private static final String DEFAULT_PATH = ".";
    private static final String CONFIG_DIR = "config-dir=";
    public static final String EXPORT_TO_FILE_ERROR = "Export to files not enabled, please set apoc.export.file.enabled=true in your apoc.conf.\n" +
            "Otherwise, if you are running in a cloud environment without filesystem access, use the `{stream:true}` config and null as a 'file' parameter to stream the export back to your client.\n" +
            "Note that the stream mode cannot be used with the apoc.export.xls.* procedures.";

    private final Config neo4jConfig;
    private final Log log;
    private final DatabaseManagementService databaseManagementService;

    private Configuration config;

    private static ApocConfig theInstance;
    private LoggingType loggingType;
    private SimpleRateLimiter rateLimiter;
    private GraphDatabaseService systemDb;

    private List<IPAddressString> blockedIpRanges = List.of();

    /**
     * keep track if this instance is already initialized so dependent class can wait if needed
     */
    private boolean initialized = false;

    public ApocConfig(Config neo4jConfig, LogService log, GlobalProcedures globalProceduresRegistry, DatabaseManagementService databaseManagementService) {
        this.neo4jConfig = neo4jConfig;
        this.blockedIpRanges = neo4jConfig.get(ApocSettings.cypher_ip_blocklist);
        this.log = log.getInternalLog(ApocConfig.class);
        this.databaseManagementService = databaseManagementService;
        theInstance = this;

        // expose this config instance via `@Context ApocConfig config`
        globalProceduresRegistry.registerComponent((Class<ApocConfig>) getClass(), ctx -> this, true);
        this.log.info("successfully registered ApocConfig for @Context");
    }

    // use only for unit tests
    public ApocConfig() {
        this.neo4jConfig = null;
        this.log = NullLog.getInstance();
        this.databaseManagementService = null;
        theInstance = this;
        this.config = new PropertiesConfiguration();
    }

    public Configuration getConfig() {
        return config;
    }

    @Override
    public void init() throws Exception {
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

            // copy apoc settings from neo4j.conf for legacy support
            neo4jConfig.getDeclaredSettings().entrySet().stream()
                    .filter(e -> !config.containsKey(e.getKey()))
                    .filter(e -> e.getKey().startsWith("apoc."))
                    .forEach(e -> {
                        log.info("setting from neo4j.conf: " + e.getKey() + "=" + neo4jConfig.get(e.getValue()));
                        config.setProperty(e.getKey(), neo4jConfig.get(e.getValue()));
                    });

            addDbmsDirectoriesMetricsSettings();
            for (Setting s : NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES) {
                Object value = neo4jConfig.get(s);
                if (value!=null) {
                    config.setProperty(s.name(), value.toString());
                }
            }

            boolean allowFileUrls = neo4jConfig.get(GraphDatabaseSettings.allow_file_urls);
            config.setProperty(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, allowFileUrls);
            
            // todo - evaluate default timezone here [maybe is reusable], otherwise through db.execute('CALL dbms.listConfig()')
            final Setting<ZoneId> db_temporal_timezone = GraphDatabaseSettings.db_temporal_timezone;
            config.setProperty(db_temporal_timezone.name(), neo4jConfig.get(db_temporal_timezone));

            initLogging();
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    private void addDbmsDirectoriesMetricsSettings() {
        try {
            Class<?> metricsSettingsClass = Class.forName("com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings");
            Field csvPathField = metricsSettingsClass.getDeclaredField("csvPath");
            Setting<Path> dbms_directories_metrics = (Setting<Path>) csvPathField.get(null);
            NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES.add(dbms_directories_metrics);
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            // ignore - on community edition that class does not exist
        }
    }

    public LoggingType getLoggingType() {
        return loggingType;
    }

    public SimpleRateLimiter getRateLimiter() {
        return rateLimiter;
    }

    public void setLoggingType(LoggingType loggingType) {
        this.loggingType = loggingType;
    }

    public void setRateLimiter(SimpleRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    public GraphDatabaseService getSystemDb() {
        if (systemDb == null) {
            try {
                systemDb = databaseManagementService.database(SYSTEM_DATABASE_NAME);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return systemDb;
    }

    public enum LoggingType {none, safe, raw}

    private void initLogging() {
        loggingType = LoggingType.valueOf(getString("apoc.user.log.type", "safe").trim());
        rateLimiter = new SimpleRateLimiter(getInt( "apoc.user.log.window.time", 10000), getInt("apoc.user.log.window.ops", 10));
    }

    // added because with binary file there isn't an url
    public void isImportFileEnabled() {
        if (!config.getBoolean(APOC_IMPORT_FILE_ENABLED)) {
            throw new RuntimeException(LOAD_FROM_FILE_ERROR);
        }
    }

    /*
        TODO
        This needs to be cleaned up in 5.0
        WebURLAccessRule was added in the database in 4.4.5, so it would
        break backwards compatibility with 4.4.xx previous versions
    */
    private void checkAllowedUrl(String url) throws IOException {
        try {
            if (blockedIpRanges != null && !blockedIpRanges.isEmpty()) {
                URL parsedUrl = new URL(url);
                InetAddress inetAddress = InetAddress.getByName(parsedUrl.getHost());

                for (var blockedIpRange : blockedIpRanges)
                {
                    if (blockedIpRange.contains(new IPAddressString(inetAddress.getHostAddress())))
                    {
                        throw new IOException("access to " + inetAddress + " is blocked via the configuration property "
                                               + ApocSettings.cypher_ip_blocklist.name());
                    }
                }
            }
        } catch (MalformedURLException e) {
            throw new IOException(e);
        }
    }

    public void checkReadAllowed(String url) throws IOException {
        if (isFile(url)) {
            isImportFileEnabled();
        } else {
            checkAllowedUrl(url);
        }
    }

    public void checkWriteAllowed(ExportConfig exportConfig, String fileName) {
        if (!config.getBoolean(APOC_EXPORT_FILE_ENABLED)) {
            if (exportConfig == null || (fileName != null && !fileName.equals("")) || !exportConfig.streamStatements()) {
                throw new RuntimeException(EXPORT_TO_FILE_ERROR);
            }
        }
    }

    public static ApocConfig apocConfig() {
        return theInstance;
    }


    /*
     * delegate methods for Configuration
     */

    public Iterator<String> getKeys(String prefix) {
        return getConfig().getKeys(prefix);
    }

    public boolean containsKey(String key) {
        return getConfig().containsKey(key);
    }

    public String getString(String key) {
        return getConfig().getString(key);
    }

    public String getString(String key, String defaultValue) {
        return getConfig().getString(key, defaultValue);
    }

    public <T> void setProperty(Setting<T> key, T value) {
        getConfig().setProperty(key.name(), value);
    }

    public void setProperty(String key, Object value) {
        getConfig().setProperty(key, value);
    }

    public boolean getBoolean(String key) {
        return getConfig().getBoolean(key);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return getConfig().getBoolean(key, defaultValue);
    }

    public <T extends Enum<T>> T getEnumProperty(String key, Class<T> cls, T defaultValue) {
        var value = getConfig().getString(key, defaultValue.toString()).trim();
        try {
            return T.valueOf(cls, value);
        } catch (IllegalArgumentException e) {
            log.error("Wrong value '{}' for parameter '{}' is provided. Default value is used: '{}'", value, key, defaultValue);
            return defaultValue;
        }
    }

    public boolean isImportFolderConfigured() {
        // in case we're test database import path is TestDatabaseManagementServiceBuilder.EPHEMERAL_PATH

        String importFolder = getImportDir();
        if (importFolder==null) {
            return false;
        } else {
            return !"/target/test data/neo4j".equals(importFolder);
        }
    }

    public String getImportDir() {
        return apocConfig().getString("dbms.directories.import");
    }

    public int getInt(String key, int defaultValue) {
        try {
            return getConfig().getInt(key, defaultValue);
        } catch (ConversionException e) {
            Object o = getConfig().getProperty(key);
            if (o instanceof Duration) {
                return (int) ((Duration)o).getSeconds();
            } else {
                throw new IllegalArgumentException("don't know how to convert for config option " + key, e);
            }
        }
    }
}
