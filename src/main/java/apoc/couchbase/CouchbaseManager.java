package apoc.couchbase;

import com.couchbase.client.java.auth.PasswordAuthenticator;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.apache.commons.configuration2.Configuration;
import org.neo4j.internal.helpers.collection.Pair;
import org.parboiled.common.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static apoc.ApocConfig.apocConfig;

/**
 * Creates a {@link CouchbaseConnection} though that all of the operations
 * performed against a Couchbase Server can be done.
 *
 * @author inserpio
 * @since 15.8.2016
 */
public class CouchbaseManager {

    protected static final String COUCHBASE_CONFIG_KEY = "couchbase.";

    protected static final String USERNAME_CONFIG_KEY = "username";

    protected static final String PASSWORD_CONFIG_KEY = "password";

    protected static final String URI_CONFIG_KEY = "uri";

    protected static final String PORT_CONFIG_KEY = "port";

    private static final Map<String, Object> DEFAULT_CONFIG;

    static {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("connectTimeout", 5000L);
        cfg.put("socketConnectTimeout", 1500);
        cfg.put("kvTimeout", 2500);
        cfg.put("ioPoolSize", 3);
        cfg.put("computationPoolSize", 3);
        DEFAULT_CONFIG = Collections.unmodifiableMap(cfg);

    }

    protected CouchbaseManager() {
    }


    protected static URI checkAndGetURI(String hostOrKey) {
        URI uri = URI.create(hostOrKey);

        if (!StringUtils.isEmpty(uri.getScheme())) {
            String userInfo = uri.getUserInfo();
            if (StringUtils.isEmpty(userInfo)) {
                throw new RuntimeException("URI must include credentials otherwise use apoc.couchbase.<key>.* configuration");
            }
            String[] infoTokens = userInfo.split(":");
            if (infoTokens.length != 2) {
                throw new RuntimeException("Credentials must be defined according URI specifications");
            }

            return uri;
        }

        return null;
    }

    /**
     * Creates a {@link Pair} containing a {@link PasswordAuthenticator} and a {@link List} of (cluster) nodes from configuration properties
     *
     * @param configurationKey the configuration key in neo4j.conf that should be defined as apoc.couchbase.[configurationKey]
     * @return a tuple2, the connections objects that we need to establish a connection to a Couchbase Server
     */
    protected static Pair<PasswordAuthenticator, List<String>> getConnectionObjectsFromConfigurationKey(String configurationKey) {
        Configuration couchbaseConfig = getKeyMap(configurationKey);

        Object username, password;
        if ((username = couchbaseConfig.getString(USERNAME_CONFIG_KEY)) == null || (password = couchbaseConfig.getString(PASSWORD_CONFIG_KEY)) == null) {
            throw new RuntimeException("Please check you 'apoc.couchbase." + configurationKey + "' configuration, username and password are missing");
        }

        Object url;
        if ((url = couchbaseConfig.getString(URI_CONFIG_KEY)) == null) {
            throw new RuntimeException("Please check you 'apoc.couchbase." + configurationKey + "' configuration, url is missing");
        }

        return Pair.of(
                new PasswordAuthenticator(username.toString(), password.toString()),
                Arrays.asList(url.toString().split(","))
        );
    }

    /**
     * Creates a {@link Pair} containing a {@link PasswordAuthenticator} and a {@link List} of (cluster) nodes from a URI
     *
     * @param host a URI representing the connection to a single instance, for example couchbase://username:password@hostname:port
     * @return a tuple2, the connections objects that we need to establish a connection to a Couchbase Server
     */
    protected static Pair<PasswordAuthenticator, List<String>> getConnectionObjectsFromHost(URI host) {
        List<String> nodes = Collections.emptyList();
        try {
            nodes = Arrays.asList(new URI(host.getScheme(),
                    null, host.getHost(), host.getPort(),
                    null, null, null).toString());
        } catch (URISyntaxException e) {

        }
        String[] credentials = host.getUserInfo().split(":");

        return Pair.of(
                new PasswordAuthenticator(credentials[0], credentials[1]),
                nodes
        );
    }

    /**
     * Creates a {@link Pair} containing a {@link PasswordAuthenticator} and a {@link List} of (cluster) nodes from configuration properties or a URI
     * This method verifies if the variable hostOrKey has "couchbase" as a scheme if not then it consider hostOrKey as a configuration key
     * If it's a URI then the credentials should be defined according to the URI specifications
     *
     * @param hostOrKey a configuration key (in the neo4j.conf file) or a URI
     * @return
     */
    protected static Pair<PasswordAuthenticator, List<String>> getConnectionObjectsFromHostOrKey(String hostOrKey) {
        // Check if hostOrKey is really a host, if it's a host, then we let only one host!!
        URI singleHostURI = checkAndGetURI(hostOrKey);

        // No scheme defined so it's considered a configuration key
        if (singleHostURI == null || singleHostURI.getScheme() == null) {
            return getConnectionObjectsFromConfigurationKey(hostOrKey);
        } else {
            return getConnectionObjectsFromHost(singleHostURI);
        }
    }

    /**
     * @param hostOrKey
     * @param bucketName
     * @return
     */
    public static CouchbaseConnection getConnection(String hostOrKey, String bucketName) {
        PasswordAuthenticator passwordAuthenticator = getPasswordAuthenticator(hostOrKey);
        List<String> nodes = getNodes(hostOrKey);
        DefaultCouchbaseEnvironment env = getEnv(hostOrKey);


        String[] bucketCredentials = bucketName.split(":");
        return new CouchbaseConnection(nodes, passwordAuthenticator, bucketCredentials[0], bucketCredentials.length == 2 ? bucketCredentials[1] : null, env);
    }

    private static DefaultCouchbaseEnvironment getEnv(String hostOrKey) {
        URI singleHostURI = checkAndGetURI(hostOrKey);

        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();

        builder.connectTimeout(Long.parseLong(getConfig("connectTimeout")));
        builder.socketConnectTimeout(Integer.parseInt(getConfig("socketConnectTimeout")));
        builder.kvTimeout(Integer.parseInt(getConfig("kvTimeout")));
        builder.ioPoolSize(Integer.parseInt(getConfig("ioPoolSize")));
        builder.computationPoolSize(Integer.parseInt(getConfig("computationPoolSize")));
        if (singleHostURI == null || singleHostURI.getScheme() == null) {
            Configuration couchbaseConfig = getKeyMap(hostOrKey);

            int port;
            if (( port = couchbaseConfig.getInt(PORT_CONFIG_KEY, -1)) != -1) {
                builder.bootstrapHttpDirectPort(port);
            }
        } else {
            if (singleHostURI.getPort() != -1) {
                builder.bootstrapHttpDirectPort(singleHostURI.getPort());
            }
        }
        return builder.build();
    }

    private static List<String> getNodes(String hostOrKey) {
        URI singleHostURI = checkAndGetURI(hostOrKey);
        String url;
        if (singleHostURI == null || singleHostURI.getScheme() == null) {
            Configuration couchbaseConfig = getKeyMap(hostOrKey);

            url = couchbaseConfig.getString(URI_CONFIG_KEY, null);
            if (url == null)  {
                throw new RuntimeException("Please check you 'apoc.couchbase." + hostOrKey + "' configuration, url is missing");
            }
        } else {
            url = singleHostURI.getHost();
        }
        return Arrays.asList(url.split(","));
    }

    private static PasswordAuthenticator getPasswordAuthenticator(String hostOrKey) {
        URI singleHostURI = checkAndGetURI(hostOrKey);

        if (singleHostURI == null || singleHostURI.getScheme() == null) {
            Configuration couchbaseConfig = getKeyMap(hostOrKey);

            Object username, password;
            if ((username = couchbaseConfig.getString(USERNAME_CONFIG_KEY)) == null || (password = couchbaseConfig.getString(PASSWORD_CONFIG_KEY)) == null) {
                throw new RuntimeException("Please check you 'apoc.couchbase." + hostOrKey + "' configuration, username and password are missing");
            }

            return new PasswordAuthenticator(username.toString(), password.toString());
        } else {
            String[] userInfo = singleHostURI.getUserInfo().split(":");
            return new PasswordAuthenticator(userInfo[0], userInfo[1]);
        }
    }

    private static Configuration getKeyMap(String hostOrKey) {
        Configuration couchbaseConfig = apocConfig().getConfig().subset("apoc." + COUCHBASE_CONFIG_KEY + hostOrKey);

        if (couchbaseConfig.isEmpty()) {
            throw new RuntimeException("Please check neo4j.conf file 'apoc.couchbase." + hostOrKey + "' is missing");
        }

        return couchbaseConfig;
    }

    public static String getConfig(String key) {
        return apocConfig().getString("apoc." + COUCHBASE_CONFIG_KEY + key, DEFAULT_CONFIG.get(key).toString());
    }

}
