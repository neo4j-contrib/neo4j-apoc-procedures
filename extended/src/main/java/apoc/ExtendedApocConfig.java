package apoc;

import apoc.export.util.ExportConfigExtended;
import apoc.util.FileUtilsExtended;
import apoc.util.SimpleRateLimiter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.configuration2.CombinedConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.EnvironmentConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SystemConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.ex.ConversionException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.configuration2.tree.OverrideCombiner;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.util.Preconditions;

import static java.lang.String.format;
import static org.neo4j.configuration.BootloaderSettings.lib_directory;
import static org.neo4j.configuration.BootloaderSettings.run_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.load_csv_file_url_root;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.internal.helpers.ProcessUtils.executeCommandWithOutput;

public class ExtendedApocConfig extends LifecycleAdapter
{
    public static final String SUN_JAVA_COMMAND = "sun.java.command";
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
    public static final String APOC_ML_OPENAI_URL = "apoc.ml.openai.url";
    public static final String APOC_ML_OPENAI_TYPE = "apoc.ml.openai.type";
    public static final String APOC_ML_OPENAI_AZURE_VERSION = "apoc.ml.azure.api.version";
    public static final String APOC_ML_VERTEXAI_URL = "apoc.ml.vertexai.url";
    public static final String APOC_ML_WATSON_PROJECT_ID = "apoc.ml.watson.project.id";
    public static final String APOC_ML_WATSON_URL = "apoc.ml.watson.url";
    public static final String APOC_AWS_KEY_ID = "apoc.aws.key.id";
    public static final String APOC_AWS_SECRET_KEY = "apoc.aws.secret.key";
    
    // From core
    public static final String APOC_IMPORT_FILE_ENABLED = "apoc.import.file.enabled";
    public static final String APOC_EXPORT_FILE_ENABLED = "apoc.export.file.enabled";
    public static final String APOC_IMPORT_FILE_USE_NEO4J_CONFIG = "apoc.import.file.use_neo4j_config";
    public static final String APOC_TRIGGER_ENABLED = "apoc.trigger.enabled";
    public static final String APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM =
            "apoc.import.file.allow_read_from_filesystem";
    public static final String APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS = "apoc.jobs.scheduled.num_threads";
    public static final String APOC_CONFIG_JOBS_POOL_NUM_THREADS = "apoc.jobs.pool.num_threads";
    public static final String APOC_CONFIG_JOBS_QUEUE_SIZE = "apoc.jobs.queue.size";
    public static final String APOC_CONFIG_INITIALIZER = "apoc.initializer";
    public static final String LOAD_FROM_FILE_ERROR =
            "Import from files not enabled, please set apoc.import.file.enabled=true in your apoc.conf";
    public static final String APOC_MAX_DECOMPRESSION_RATIO = "apoc.max.decompression.ratio";
    public static final Integer DEFAULT_MAX_DECOMPRESSION_RATIO = 200;
    
    public enum UuidFormatType { hex, base64 }

    // These were earlier added via the Neo4j config using the ApocSettings.java class
    private static final Map<String,Object> configDefaultValues =
            Map.of(
                    APOC_EXPORT_FILE_ENABLED, false,
                    APOC_IMPORT_FILE_ENABLED, false,
                    APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true,
                    APOC_TRIGGER_ENABLED, false,
                    APOC_TTL_SCHEDULE, Duration.ofMinutes(1),
                    APOC_TTL_ENABLED, false,
                    APOC_TTL_LIMIT, 1000L,
                    APOC_UUID_ENABLED, false
            );

    private static final List<Setting<?>> NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES = new ArrayList<>(Arrays.asList(
            data_directory,
            load_csv_file_url_root,
            logs_directory,
            plugin_dir,
            transaction_logs_root_path,
            run_directory,
            lib_directory,
            neo4j_home));

    public static final String CONFIG_DIR = "config-dir=";
    public static final String EXPORT_NOT_ENABLED_ERROR =
            "Export to files not enabled, please set apoc.export.file.enabled=true in your apoc.conf.";
    public static final String EXPORT_TO_FILE_ERROR = EXPORT_NOT_ENABLED_ERROR
            + "\nOtherwise, if you are running in a cloud environment without filesystem access, use the `{stream:true}` config and null as a 'file' parameter to stream the export back to your client.";

    private final Config neo4jConfig;
    private final Log log;
    private final DatabaseManagementService databaseManagementService;

    private final String defaultConfigPath;

    private Configuration config;

    private static ExtendedApocConfig theInstance;

    private ExtendedApocConfig.LoggingType loggingType;
    private SimpleRateLimiter rateLimiter;

    private GraphDatabaseService systemDb;
    private boolean expandCommands;
    private Duration commandEvaluationTimeout;
    private File apocConfFile;
    /**
     * keep track if this instance is already initialized so dependent class can wait if needed
     */
    private boolean initialized = false;

    public ExtendedApocConfig(
                              Config neo4jConfig,
                              LogService log, 
                              GlobalProcedures globalProceduresRegistry, 
                              String defaultConfigPath,
                              DatabaseManagementService databaseManagementService) {
        this.neo4jConfig = neo4jConfig;
        this.commandEvaluationTimeout =
                neo4jConfig.get(GraphDatabaseInternalSettings.config_command_evaluation_timeout);
        if (this.commandEvaluationTimeout == null) {
            this.commandEvaluationTimeout =
                    GraphDatabaseInternalSettings.config_command_evaluation_timeout.defaultValue();
        }
        this.expandCommands = neo4jConfig.expandCommands();
        this.log = log.getInternalLog(ExtendedApocConfig.class);
        this.defaultConfigPath = defaultConfigPath;
        theInstance = this;
        this.databaseManagementService = databaseManagementService;

        // expose this config instance via `@Context ExtendedApocConfig config`
        globalProceduresRegistry.registerComponent(ExtendedApocConfig.class, ctx -> this, true);
        this.log.info("successfully registered ExtendedApocConfig for @Context");
    }

    @Override
    public void init() {
        log.debug("called init");
        // grab NEO4J_CONF from environment. If not set, calculate it from sun.java.command system property or Neo4j default
        String neo4jConfFolder = System.getenv().getOrDefault("NEO4J_CONF", determineNeo4jConfFolder());
        System.setProperty("NEO4J_CONF", neo4jConfFolder);
        log.info("system property NEO4J_CONF set to %s", neo4jConfFolder);
        File apocConfFile = new File(neo4jConfFolder + "/apoc.conf");
        // Command Expansion required check from Neo4j
        if (apocConfFile.exists() && this.expandCommands) {
            Config.Builder.validateFilePermissionForCommandExpansion(List.of(apocConfFile.toPath()));
        }

        loadConfiguration(apocConfFile);
        initialized = true;
    }

    private String evaluateIfCommand(String settingName, String entry) {
        if (Config.isCommand(entry)) {
            Preconditions.checkArgument(
                    expandCommands,
                    format(
                            "%s is a command, but config is not explicitly told to expand it. (Missing --expand-commands argument?)",
                            entry));
            String str = entry.trim();
            String command = str.substring(2, str.length() - 1);
            log.info("Executing external script to retrieve value of setting " + settingName);
            return executeCommandWithOutput(command, commandEvaluationTimeout);
        }
        return entry;
    }

    public boolean isInitialized() {
        return initialized;
    }

    protected String determineNeo4jConfFolder() {
        String command = System.getProperty(SUN_JAVA_COMMAND);
        if (command == null) {
            log.warn(
                    "system property %s is not set, assuming %s as conf dir. This might cause `apoc.conf` not getting loaded.",
                    SUN_JAVA_COMMAND, defaultConfigPath);
            return defaultConfigPath;
        } else {
            final String neo4jConfFolder = Stream.of(command.split("--"))
                    .map(String::trim)
                    .filter(s -> s.startsWith(CONFIG_DIR))
                    .map(s -> s.substring(CONFIG_DIR.length()))
                    .findFirst()
                    .orElse(defaultConfigPath);
            if (defaultConfigPath.equals(neo4jConfFolder)) {
                log.info("cannot determine conf folder from sys property %s, assuming %s", command, defaultConfigPath);
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

    protected void loadConfiguration(File apocConfFile) {
        try {
            config = setupConfigurations(apocConfFile);

            // Command Expansion if needed
            config.getKeys()
                    .forEachRemaining(configKey -> config.setProperty(
                            configKey,
                            evaluateIfCommand(
                                    configKey, config.getProperty(configKey).toString())));
            
            // set config settings not explicitly set in apoc.conf to their default value
            configDefaultValues.forEach((k,v) -> {
                if (!config.containsKey(k))
                {
                   config.setProperty(k, v);
                   log.info("setting APOC config to default value: " + k + "=" + v);
                }
            });
            
            for (Setting<?> s : NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES) {
                Object value = neo4jConfig.get(s);
                if (value != null) {
                    config.setProperty(s.name(), value.toString());
                }
            }

            if (!config.containsKey(APOC_MAX_DECOMPRESSION_RATIO)) {
                config.setProperty(APOC_MAX_DECOMPRESSION_RATIO, DEFAULT_MAX_DECOMPRESSION_RATIO);
            }
            if (config.getInt(APOC_MAX_DECOMPRESSION_RATIO) == 0) {
                throw new IllegalArgumentException(
                        format("value 0 is not allowed for the config option %s", APOC_MAX_DECOMPRESSION_RATIO));
            }

            boolean allowFileUrls = neo4jConfig.get(GraphDatabaseSettings.allow_file_urls);
            config.setProperty(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, allowFileUrls);

            // todo - evaluate default timezone here [maybe is reusable], otherwise through db.execute('CALL
            // dbms.listConfig()')
            final Setting<ZoneId> db_temporal_timezone = GraphDatabaseSettings.db_temporal_timezone;
            config.setProperty(db_temporal_timezone.name(), neo4jConfig.get(db_temporal_timezone));

            initLogging();
        } catch ( ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    private static Configuration setupConfigurations(File propertyFile) throws ConfigurationException {
        PropertiesConfiguration configFile = new PropertiesConfiguration();
        if (propertyFile.exists()) {
            final FileHandler handler = new FileHandler(configFile);
            handler.setFile(propertyFile);
            handler.load();
        }

        // OverrideCombiner will evaluate keys in order, i.e. env before sys etc.
        CombinedConfiguration combined = new CombinedConfiguration();
        combined.setNodeCombiner(new OverrideCombiner());
        combined.addConfiguration(new EnvironmentConfiguration());
        combined.addConfiguration(new SystemConfiguration());
        combined.addConfiguration(configFile);

        return combined;
    }

    public Configuration getConfig() {
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

    public boolean getBoolean(String key) {
        return getConfig().getBoolean(key);
    }
    
    public void checkReadAllowed(String url, URLAccessChecker urlAccessChecker) throws IOException {
        if (FileUtilsExtended.isFile(url)) {
            isImportFileEnabled();
        } else {
            checkAllowedUrlAndPinToIP(url, urlAccessChecker);
        }
    }

    // added because with binary file there isn't an url
    public void isImportFileEnabled() {
        if (!config.getBoolean(APOC_IMPORT_FILE_ENABLED)) {
            throw new RuntimeException(LOAD_FROM_FILE_ERROR);
        }
    }
    
    public URL checkAllowedUrlAndPinToIP(String url, URLAccessChecker urlAccessChecker) throws IOException {
        try {
            URL parsedUrl = new URL(url);
            return urlAccessChecker.checkURL(parsedUrl);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    
    public boolean isImportFolderConfigured() {
        // in case we're test database import path is TestDatabaseManagementServiceBuilder.EPHEMERAL_PATH

        String importFolder = getImportDir();
        if (importFolder == null) {
            return false;
        } else {
            return !"/target/test data/neo4j".equals(importFolder);
        }
    }
    
    public String getImportDir() {
        return extendedApocConfig().getString("server.directories.import");
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
    
    public void setProperty(String key, Object value) {
        getConfig().setProperty(key, value);
    }

    public int getInt(String key, int defaultValue) {
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


    // Methods brought over from Core Config

    public String getString(String key) {
        return getConfig().getString(key);
    }

    public void checkWriteAllowed(ExportConfigExtended exportConfig, String fileName) {
        if (!config.getBoolean(APOC_EXPORT_FILE_ENABLED)) {
            if (exportConfig == null || (fileName != null && !fileName.isEmpty()) || !exportConfig.streamStatements()) {
                throw new RuntimeException(EXPORT_TO_FILE_ERROR);
            }
        }
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
}
