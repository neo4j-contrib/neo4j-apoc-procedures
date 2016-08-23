package apoc.couchbase;

import java.util.Arrays;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseAsyncCluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/**
 * Creates a {@link CouchbaseConnection} though that all of the operations
 * performed against a Couchbase Server can be done.
 *
 * @since 15.8.2016
 * @author inserpio
 */
public class CouchbaseManager {

	public static final DefaultCouchbaseEnvironment DEFAULT_COUCHBASE_ENVIRONMENT = DefaultCouchbaseEnvironment.create();

  protected CouchbaseManager() {
	}

	/**
	 * Opens a connection to the Couchbase Server. Behind the scenes it first
	 * creates a new {@link Cluster} reference against the <code>nodes</code>
	 * passed in and secondly opens the {@link Bucket} with the provided
	 * <code>bucketName</code>.
	 * 
	 * @param nodes
	 *          the list of nodes to use when connecting to the cluster reference;
	 *          if null is passed then it will connect to a cluster listening on
	 *          "localhost"
	 * @param bucketName
	 *          the name of the bucket to open; if null is passed then it's used
	 *          the "default" bucket name
	 * @return the opened {@link CouchbaseConnection}
	 * @see CouchbaseCluster#create(List)
	 */
	public static CouchbaseConnection getConnection(List<String> nodes, String bucketName) {
		if (nodes == null) {
			nodes = Arrays.asList(CouchbaseAsyncCluster.DEFAULT_HOST);
		}
		return new CouchbaseConnection(CouchbaseCluster.create(DEFAULT_COUCHBASE_ENVIRONMENT, nodes), bucketName);
	}
}
