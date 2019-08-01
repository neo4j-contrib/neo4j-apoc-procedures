package apoc;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.combined.CombinedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApocConfig extends LifecycleAdapter {

    public static final String SUN_JAVA_COMMAND = "sun.java.command";
    public static final Pattern CONF_DIR_PATTERN = Pattern.compile("--config-dir=(\\S+)");

    private final ExtensionContext context;
    private final Config neo4jConfig;
    private final Log log;

    private Configuration config;


    public ApocConfig(ExtensionContext context, Config neo4jConfig, LogService log) {
        this.context = context;
        this.neo4jConfig = neo4jConfig;
        this.log = log.getInternalLog(ApocConfig.class);
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
            throw new IllegalStateException("cannot determine conf folder from sys property  " + command );
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
    }
}
