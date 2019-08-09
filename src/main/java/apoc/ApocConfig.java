package apoc;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.combined.CombinedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static apoc.util.FileUtils.isFile;
import static org.neo4j.configuration.GraphDatabaseSettings.*;

public class ApocConfig extends LifecycleAdapter {

    public static final String SUN_JAVA_COMMAND = "sun.java.command";
    public static final Pattern CONF_DIR_PATTERN = Pattern.compile("--config-dir=(\\S+)");
    public static final String APOC_IMPORT_FILE_ENABLED = "apoc.import.file.enabled";
    public static final String APOC_EXPORT_FILE_ENABLED = "apoc.export.file.enabled";
    public static final String APOC_IMPORT_FILE_USE_NEO4J_CONFIG = "apoc.import.file.use_neo4j_config";
    public static final String APOC_TTL_SCHEDULE = "apoc.ttl.schedule";
    public static final String APOC_TTL_ENABLED = "apoc.ttl.enabled";
    public static final String APOC_TTL_LIMIT = "apoc.ttl.limit";
    public static final String APOC_TRIGGER_ENABLED = "apoc.trigger.enabled";
    public static final String APOC_UUID_ENABLED = "apoc.uuid.enabled";
    public static final String APOC_JSON_ZIP_URL = "apoc.json.zip.url";
    public static final String APOC_JSON_SIMPLE_JSON_URL = "apoc.json.simpleJson.url";
    public static final String APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM = "apoc.import.file.allow_read_from_filesystem";

    public static final List<Setting> NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES = Arrays.asList(
            legacy_certificates_directory,
            data_directory,
            load_csv_file_url_root,
            logs_directory,
            plugin_dir,
            logical_logs_location,
            transaction_logs_root_path,
            neo4j_home
    );

// old settings from 3.x that seem to no longer be existent
//"dbms.directories.lib",
//            "dbms.directories.metrics",  // metrics is only in EE
//            "dbms.directories.run",

    private final Config neo4jConfig;
    private final Log log;
    private final GlobalProceduresRegistry globalProceduresRegistry;

    private Configuration config;

    private static ApocConfig theInstance;

    public ApocConfig(Config neo4jConfig, LogService log, GlobalProceduresRegistry globalProceduresRegistry) {
        this.neo4jConfig = neo4jConfig;
        this.log = log.getInternalLog(ApocConfig.class);
        this.globalProceduresRegistry = globalProceduresRegistry;
        theInstance = this;
    }

    public Configuration getConfig() {
        return config;
    }

    @Override
    public void start() throws Exception {
        // grab NEO4J_CONF from environment. If not set, calculate it from sun.java.command system property
        String neo4jConfFolder = System.getenv().getOrDefault("NEO4J_CONF", determineNeo4jConfFolder());
        System.setProperty("NEO4J_CONF", neo4jConfFolder);
        log.info("system property NEO4J_CONF set to %s", neo4jConfFolder);

        // expose this config instance via `@Context ApocConfig config`
        if (globalProceduresRegistry!=null) {
            globalProceduresRegistry.registerComponent((Class<ApocConfig>) getClass(), ctx -> this, true);
        }

        loadConfiguration();
    }

    protected String determineNeo4jConfFolder() {
        // sun.java.command=com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02/conf
        String command = System.getProperty(SUN_JAVA_COMMAND);
        Matcher matcher = CONF_DIR_PATTERN.matcher(command);
        if (matcher.find()) {
            String neo4jConfFolder = matcher.group(1);
            log.info("from system properties: NEO4J_CONF=%s", neo4jConfFolder);
            return neo4jConfFolder;
        } else {
            log.info("cannot determine conf folder from sys property %s, assuming '.' ", command);
            return ".";
        }
    }

    /**
     * use apache commons to load configuration
     * classpath:/apoc-config.xml contains a description where to load configuration from
     * @throws org.apache.commons.configuration2.ex.ConfigurationException
     */
    protected void loadConfiguration() throws org.apache.commons.configuration2.ex.ConfigurationException {
        URL resource = getClass().getClassLoader().getResource("apoc-config.xml");
        log.info("loading apoc meta config from %s", resource.toString());
        CombinedConfigurationBuilder builder = new CombinedConfigurationBuilder()
                .configure(new Parameters().fileBased().setURL(resource));
        config = builder.getConfiguration();

        // copy apoc settings from neo4j.conf for legacy support
        neo4jConfig.getDeclaredSettings().entrySet().stream()
                .filter(e -> e.getKey().startsWith("apoc."))
                .forEach(e -> config.setProperty(e.getKey(), neo4jConfig.get(e.getValue())));

        for (Setting s : NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES) {
            config.setProperty(s.name(), neo4jConfig.get(s).toString());
        }

        boolean allowFileUrls = neo4jConfig.get(GraphDatabaseSettings.allow_file_urls);
        config.setProperty(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, allowFileUrls);
    }

    public void checkReadAllowed(String url) {
        if (isFile(url) && !config.getBoolean(APOC_IMPORT_FILE_ENABLED)) {
            throw new RuntimeException("Import from files not enabled," +
                    " please set apoc.import.file.enabled=true in your apoc.conf");
        }
    }
    public void checkWriteAllowed() {
        if (!config.getBoolean(APOC_EXPORT_FILE_ENABLED)) {
            throw new RuntimeException("Export to files not enabled, please set apoc.export.file.enabled=true in your apoc.conf");
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

    public String getString(String key) {
        return getConfig().getString(key);
    }

    public String getString(String key, String defaultValue) {
        return getConfig().getString(key, defaultValue);
    }

    public void setProperty(String key, Object value) {
        getConfig().setProperty(key, value);
    }

    public boolean getBoolean(String key) {
        return getConfig().getBoolean(key);
    }
}
