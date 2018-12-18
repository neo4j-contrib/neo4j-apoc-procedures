package apoc.couchbase;

import apoc.ApocConfiguration;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseAsyncCluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.PasswordAuthenticator;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import org.neo4j.helpers.collection.Pair;
import org.parboiled.common.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Creates a {@link CouchbaseConnection} though that all of the operations
 * performed against a Couchbase Server can be done.
 *
 * @author inserpio
 * @since 15.8.2016
 */
public class CouchbaseManager {

    public static final DefaultCouchbaseEnvironment DEFAULT_COUCHBASE_ENVIRONMENT = DefaultCouchbaseEnvironment.builder().retryStrategy(FailFastRetryStrategy.INSTANCE).build();

    protected static final String COUCHBASE_CONFIG_KEY = "couchbase.";

    protected static final String USERNAME_CONFIG_KEY = "username";

    protected static final String PASSWORD_CONFIG_KEY = "password";

    protected static final String URI_CONFIG_KEY = "uri";

    protected static final String PORT_CONFIG_KEY = "port";

    protected CouchbaseManager() {
    }

    /**
     * Opens a connection to the Couchbase Server. Behind the scenes it first
     * creates a new {@link Cluster} reference against the <code>nodes</code>
     * passed in and secondly opens the {@link Bucket} with the provided
     * <code>bucketName</code>.
     *
     * @param nodes      the list of nodes to use when connecting to the cluster reference;
     *                   if null is passed then it will connect to a cluster listening on
     *                   "localhost"
     * @param bucketName the name of the bucket to open; if null is passed then it's used
     *                   the "default" bucket name
     * @return the opened {@link CouchbaseConnection}
     * @see CouchbaseCluster#create(List)
     */
    @Deprecated
    public static CouchbaseConnection getConnection(List<String> nodes, String bucketName) {
        if (nodes == null) {
            nodes = Arrays.asList(CouchbaseAsyncCluster.DEFAULT_HOST);
        }
        return new CouchbaseConnection(CouchbaseCluster.create(DEFAULT_COUCHBASE_ENVIRONMENT, nodes), bucketName);
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
        Map<String, Object> couchbaseConfig = getKeyMap(configurationKey);

        Object username, password;
        if ((username = couchbaseConfig.get(USERNAME_CONFIG_KEY)) == null || (password = couchbaseConfig.get(PASSWORD_CONFIG_KEY)) == null) {
            throw new RuntimeException("Please check you 'apoc.couchbase." + configurationKey + "' configuration, username and password are missing");
        }

        Object url;
        if ((url = couchbaseConfig.get(URI_CONFIG_KEY)) == null) {
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

//    /**
//     * @param hostOrKey
//     * @param bucketName
//     * @return
//     */
//    public static CouchbaseConnection getConnection(String hostOrKey, String bucketName) {
//        Pair<PasswordAuthenticator, List<String>> connectionObjects = getConnectionObjectsFromHostOrKey(hostOrKey);
//
//        String[] bucketCredentials = bucketName.split(":");
//        return new CouchbaseConnection(connectionObjects.other(), connectionObjects.first(), bucketCredentials[0], bucketCredentials.length == 2 ? bucketCredentials[1] : null, null);
//    }

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

        // No scheme defined so it's considered a configuration key
        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();
//        builder.retryStrategy(FailFastRetryStrategy.INSTANCE);
        if (singleHostURI == null || singleHostURI.getScheme() == null) {
            Map<String, Object> couchbaseConfig = getKeyMap(hostOrKey);

            Object port;
            if ((port = couchbaseConfig.get(PORT_CONFIG_KEY)) != null) {
                builder.bootstrapHttpDirectPort(Integer.parseInt(port.toString()));
            }
            return builder.build();
        } else {
            if (singleHostURI.getPort() != -1) {
                builder.bootstrapHttpDirectPort(singleHostURI.getPort());
            }
            return builder.build();
        }
    }

    private static List<String> getNodes(String hostOrKey) {
        URI singleHostURI = checkAndGetURI(hostOrKey);
        // No scheme defined so it's considered a configuration key
        Object url;
        if (singleHostURI == null || singleHostURI.getScheme() == null) {
            Map<String, Object> couchbaseConfig = getKeyMap(hostOrKey);
            if ((url = couchbaseConfig.get(URI_CONFIG_KEY)) == null) {
                throw new RuntimeException("Please check you 'apoc.couchbase." + hostOrKey + "' configuration, url is missing");
            }
        } else {
            url = singleHostURI.getHost();
        }
        return Arrays.asList(url.toString().split(","));
    }

    private static PasswordAuthenticator getPasswordAuthenticator(String hostOrKey) {
        URI singleHostURI = checkAndGetURI(hostOrKey);

        // No scheme defined so it's considered a configuration key
        if (singleHostURI == null || singleHostURI.getScheme() == null) {
            Map<String, Object> couchbaseConfig = getKeyMap(hostOrKey);

            Object username, password;
            if ((username = couchbaseConfig.get(USERNAME_CONFIG_KEY)) == null || (password = couchbaseConfig.get(PASSWORD_CONFIG_KEY)) == null) {
                throw new RuntimeException("Please check you 'apoc.couchbase." + hostOrKey + "' configuration, username and password are missing");
            }

            return new PasswordAuthenticator(username.toString(), password.toString());
        } else {
            String[] userInfo = singleHostURI.getUserInfo().split(":");
            return new PasswordAuthenticator(userInfo[0], userInfo[1]);
        }
    }

    private static Map<String, Object> getKeyMap(String hostOrKey) {
        Map<String, Object> couchbaseConfig = ApocConfiguration.get(COUCHBASE_CONFIG_KEY + hostOrKey);

        if (couchbaseConfig.isEmpty()) {
            throw new RuntimeException("Please check neo4j.conf file 'apoc.couchbase." + hostOrKey + "' is missing");
        }

        return couchbaseConfig;
    }

}
