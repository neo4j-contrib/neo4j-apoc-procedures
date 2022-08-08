package apoc;

import apoc.cache.Static;
import apoc.util.FileUtils;
import apoc.util.Util;
import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.List;

import static java.lang.String.format;
import static org.neo4j.kernel.configuration.Settings.setting;

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

    public static final String CYPHER_IP_BLOCKLIST = "unsupported.dbms.cypher_ip_blocklist";
    private static List<IPAddressString> blockedIpRanges = Arrays.asList();

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
        Map<String, String> params = neo4jConfig.getRaw();
        blockedIpRanges = neo4jConfig.get(ApocSettings.cypher_ip_blocklist);
        apocConfig.clear();
        apocConfig.putAll(Util.subMap(params, PREFIX));
        mergeDefaults();
        PARAM_WHITELIST.forEach((k, v) -> {
            Optional<Object> configValue = neo4jConfig.getValue(k);
            if (configValue.isPresent()) {
                apocConfig.put(v, configValue.get().toString());
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

    public static boolean isImportFolderConfigured() {
        return !"".equals(getImportDir());

    }

    public static String getImportDir() {
        return ApocConfiguration.get("dbms.directories.import", "").toString();
    }


    public IPAddressString parse( String value )
    {
        IPAddressString ipAddress = new IPAddressString( value.trim() );
        try
        {
            ipAddress.validate();
        }
        catch ( AddressStringException e )
        {
            throw new IllegalArgumentException( format( "'%s' is not a valid CIDR ip", value ), e );
        }
        return ipAddress;
    }


    public static void checkAllowedUrl(String url) throws IOException
    {
        try {
            if (blockedIpRanges != null && !blockedIpRanges.isEmpty()) {
                URL parsedUrl = new URL( url);
                InetAddress inetAddress = InetAddress.getByName( parsedUrl.getHost());

                for (IPAddressString blockedIpRange : blockedIpRanges)
                {
                    if (blockedIpRange.contains(new IPAddressString(inetAddress.getHostAddress())))
                    {
                        throw new IOException("access to " + inetAddress + " is blocked via the configuration property "
                                              + CYPHER_IP_BLOCKLIST);
                    }
                }
            }
        } catch ( MalformedURLException e) {
            throw new IOException(e);
        }
    }
}

class ApocSettings {
    public static final Function<String, IPAddressString> CIDR_IP = new Function<String, IPAddressString>()
    {
        @Override
        public IPAddressString apply(String value)
        {
            IPAddressString ipAddress = new IPAddressString(value.trim());
            try
            {
                ipAddress.validate();
            }
            catch (AddressStringException e)
            {
                throw new IllegalArgumentException(format( "'%s' is not a valid CIDR ip", value), e);
            }
            return ipAddress;
        }

        @Override
        public String toString()
        {
            return "an ip with subnet in CDIR format. e.g. 127.168.0.1/8";
        }
    };

    public static final Setting<List<IPAddressString>> cypher_ip_blocklist = setting(
            "unsupported.dbms.cypher_ip_blocklist", Settings.list(",", CIDR_IP), "");
}