package apoc.couchbase;

import apoc.ApocConfiguration;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseAsyncCluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.Authenticator;
import com.couchbase.client.java.auth.PasswordAuthenticator;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.parboiled.common.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Creates a {@link CouchbaseConnection} though that all of the operations
 * performed against a Couchbase Server can be done.
 *
 * @author inserpio
 * @since 15.8.2016
 */
public class CouchbaseManager {

    public static final DefaultCouchbaseEnvironment DEFAULT_COUCHBASE_ENVIRONMENT = DefaultCouchbaseEnvironment.create();

    protected static final String COUCHBASE_CONFIG_KEY = "couchbase.";

    protected static final String USERNAME_CONFIG_KEY = "username";

    protected static final String PASSWORD_CONFIG_KEY = "password";

    protected static final String URI_CONFIG_KEY = "uri";

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
     * Creates a {@link Tuple2} containing a {@link PasswordAuthenticator} and a {@link List} of (cluster) nodes from configuration properties
     *
     * @param configurationKey the configuration key in neo4j.conf that should be defined as apoc.couchbase.[configurationKey]
     * @return a tuple2, the connections objects that we need to establish a connection to a Couchbase Server
     */
    protected static Tuple2<PasswordAuthenticator, List<String>> getConnectionObjectsFromConfigurationKey(String configurationKey) {
        Map<String, Object> couchbaseConfig = ApocConfiguration.get(COUCHBASE_CONFIG_KEY + configurationKey);

        if (couchbaseConfig.isEmpty()) {
            throw new RuntimeException("Please check neo4j.conf file 'apoc.couchbase." + configurationKey + "' is missing");
        }

        Object username, password;
        if ((username = couchbaseConfig.get(USERNAME_CONFIG_KEY)) == null || (password = couchbaseConfig.get(PASSWORD_CONFIG_KEY)) == null) {
            throw new RuntimeException("Please check you 'apoc.couchbase." + configurationKey + "' configuration, username and password are missing");
        }

        Object url;
        if ((url = couchbaseConfig.get(URI_CONFIG_KEY)) == null) {
            throw new RuntimeException("Please check you 'apoc.couchbase." + configurationKey + "' configuration, url is missing");
        }

        return Tuple.create(
                new PasswordAuthenticator(username.toString(), password.toString()),
                Arrays.asList(url.toString().split(","))
        );
    }

    /**
     * Creates a {@link Tuple2} containing a {@link PasswordAuthenticator} and a {@link List} of (cluster) nodes from a URI
     *
     * @param host a URI representing the connection to a single instance, for example couchbase://username:password@hostname:port
     * @return a tuple2, the connections objects that we need to establish a connection to a Couchbase Server
     */
    protected static Tuple2<PasswordAuthenticator, List<String>> getConnectionObjectsFromHost(URI host) {
        List<String> nodes = Collections.emptyList();
        try {
            nodes = Arrays.asList(new URI(host.getScheme(),
                    null, host.getHost(), host.getPort(),
                    null, null, null).toString());
        } catch (URISyntaxException e) {

        }
        String[] credentials = host.getUserInfo().split(":");

        return Tuple.create(
                new PasswordAuthenticator(credentials[0], credentials[1]),
                nodes
        );
    }

    /**
     * Creates a {@link Tuple2} containing a {@link PasswordAuthenticator} and a {@link List} of (cluster) nodes from configuration properties or a URI
     * This method verifies if the variable hostOrKey has "couchbase" as a scheme if not then it consider hostOrKey as a configuration key
     * If it's a URI then the credentials should be defined according to the URI specifications
     *
     * @param hostOrKey a configuration key (in the neo4j.conf file) or a URI
     * @return
     */
    protected static Tuple2<PasswordAuthenticator, List<String>> getConnectionObjectsFromHostOrKey(String hostOrKey) {
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
        Tuple2<PasswordAuthenticator, List<String>> connectionObjects = getConnectionObjectsFromHostOrKey(hostOrKey);

        String[] bucketCredentials = bucketName.split(":");
        return new CouchbaseConnection(connectionObjects.v2(), connectionObjects.v1(), bucketCredentials[0], bucketCredentials.length == 2 ? bucketCredentials[1] : null);
    }
}
