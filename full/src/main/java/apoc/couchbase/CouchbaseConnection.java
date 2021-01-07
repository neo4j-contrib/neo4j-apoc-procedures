package apoc.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.PasswordAuthenticator;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.query.*;
import org.parboiled.common.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A Couchbase Server connection.
 * <p/>
 * It wraps a Couchbase {@link Bucket} instance through that all of the
 * operations performed against Couchbase are done.
 * <p/>
 * The class is {@link AutoCloseable} so that every operation can be performed
 * inside a try-block and if an exception is raised the internal state of the
 * connection object will cause {@link #close()} to safely disconnect from
 * Couchbase with no resource leaks.
 *
 * @author inserpio
 * @since 15.8.2016
 */
public class CouchbaseConnection implements AutoCloseable {

    private Cluster cluster;
    private Bucket bucket;
    private PasswordAuthenticator passwordAuthenticator;

    private CouchbaseEnvironment env;

    /**
     * @param nodes
     * @param authenticator
     * @param bucketName
     * @param bucketPassword
     * @param env
     */
    protected CouchbaseConnection(List<String> nodes, PasswordAuthenticator authenticator, String bucketName, String bucketPassword, CouchbaseEnvironment env) {
        this.env = env;
        this.cluster = CouchbaseCluster.create(env, nodes);
        this.passwordAuthenticator = authenticator;
        int couchbaseServerVersion = getMajorVersion();

        if (couchbaseServerVersion == 4) {
            /**
             * First we need to disconnect and invalidate the authentication otherwise we get a mixed-mode authentication error
             * This means that we cannot authenticate and then access to a bucket using a password because only one authentication method at time is allowed
             * Even if the bucket has no password we cannot access it after calling the authenticate method
             */
            this.cluster.disconnect();
            this.cluster = CouchbaseCluster.create(env, nodes);
            if (StringUtils.isEmpty(bucketPassword)) {
                this.bucket = cluster.openBucket(bucketName);
            } else {
                this.bucket = cluster.openBucket(bucketName, bucketPassword);
            }
        }

        /**
         * With Couchbase Server 5.x we always need to authenticate and never need to use bucketPassword because authentication and access are user based.
         * We don't need to authenticate again, it has been done checking major version
         * TODO: check if all levels of user's authorizations allow the client to get cluster info and so version as well otherwise we need two pairs of credentials
         */
        if (couchbaseServerVersion == 5 || couchbaseServerVersion == 6) {
            this.bucket = cluster.openBucket(bucketName);
        }

        if (couchbaseServerVersion < 4) {
            throw new RuntimeException("Couchbase (major) server version " + couchbaseServerVersion + " is not supported");
        }
    }

    /**
     * Get the major version of Couchbase server, that is 4.x or 5.x
     *
     * @return
     */
    protected int getMajorVersion() {
        return this.cluster.authenticate(this.passwordAuthenticator).clusterManager().info(5,TimeUnit.SECONDS).getMinVersion().major();
    }

    /**
     * Disconnect and close all buckets.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() {
//        if (!this.bucket.isClosed()) {
//            this.bucket.close();
//        }
        this.cluster.disconnect();
        this.env.shutdown();
    }

    /**
     * Retrieves a {@link JsonDocument} by its unique ID.
     *
     * @param documentId the unique ID of the document
     * @return the found {@link JsonDocument} or null if not found
     * @see Bucket#get(String)
     */
    public JsonDocument get(String documentId) {
        return this.bucket.get(documentId);
    }

    /**
     * Checks whether a {@link JsonDocument} with the given ID does exist.
     *
     * @param documentId the unique ID of the document
     * @return true if it exists, false otherwise.
     * @see Bucket#exists(String)
     */
    public boolean exists(String documentId) {
        return this.bucket.exists(documentId);
    }

    /**
     * Inserts a {@link JsonDocument} if it does not exist already.
     *
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to store
     * @return the newly created {@link JsonDocument}
     * @see Bucket#insert(Document)
     */
    public JsonDocument insert(String documentId, String json) {
        return this.bucket.insert(JsonDocument.create(documentId, JsonObject.fromJson(json)));
    }

    /**
     * Inserts or overwrites a {@link JsonDocument}.
     *
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to store
     * @return the newly created or overwritten {@link JsonDocument}
     * @see Bucket#upsert(Document)
     */
    public JsonDocument upsert(String documentId, String json) {
        return this.bucket.upsert(JsonDocument.create(documentId, JsonObject.fromJson(json)));
    }

    /**
     * Appends a {@link JsonDocument}'s content to an existing one.
     *
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to append
     * @return the updated {@link Document}
     * @see Bucket#append(Document)
     */
    public JsonDocument append(String documentId, String json) {
        this.bucket.append(JsonDocument.create(documentId, JsonObject.fromJson(json)));
        return this.get(documentId);
    }

    /**
     * Prepends a {@link JsonDocument}'s content to an existing one.
     *
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to prepend
     * @return the updated {@link Document}
     * @see Bucket#prepend(Document)
     */
    public JsonDocument prepend(String documentId, String json) {
        this.bucket.prepend(JsonDocument.create(documentId, JsonObject.fromJson(json)));
        return this.get(documentId);
    }

    /**
     * Removes the {@link JsonDocument} identified by its unique ID.
     *
     * @param documentId the unique ID of the document
     * @return the removed {@link JsonDocument}
     * @see Bucket#remove(String)
     */
    public JsonDocument remove(String documentId) {
        return this.bucket.remove(documentId);
    }

    /**
     * Replaces a {@link JsonDocument} if it does already exist.
     *
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to replace
     * @return the replaced {@link Document}
     * @see Bucket#replace(Document)
     */
    public JsonDocument replace(String documentId, String json) {
        return this.bucket.replace(JsonDocument.create(documentId, JsonObject.fromJson(json)));
    }

    /**
     * Executes a plain un-parameterized N1QL kernelTransaction.
     *
     * @param statement the raw kernelTransaction string to execute
     * @return the list of {@link JsonObject}s retrieved by this query
     * @see N1qlQuery#simple(Statement)
     */
    public List<JsonObject> executeStatement(String statement) {
        SimpleN1qlQuery query = N1qlQuery.simple(statement);
        return executeQuery(query);
    }

    /**
     * Executes a N1QL kernelTransaction with positional parameters.
     *
     * @param statement  the raw kernelTransaction string to execute (containing positional
     *                   placeholders: $1, $2, ...)
     * @param parameters the values for the positional placeholders in kernelTransaction
     * @return the list of {@link JsonObject}s retrieved by this query
     * @see N1qlQuery#parameterized(Statement, JsonArray)
     */
    public List<JsonObject> executeParametrizedStatement(String statement, List<Object> parameters) {
        JsonArray positionalParams = JsonArray.from(parameters);
        ParameterizedN1qlQuery query = N1qlQuery.parameterized(statement, positionalParams);
        return executeQuery(query);
    }

    /**
     * Executes a N1QL kernelTransaction with named parameters.
     *
     * @param statement       the raw kernelTransaction string to execute (containing named
     *                        placeholders: $param1, $param2, ...)
     * @param parameterNames  the placeholders' names in kernelTransaction
     * @param parameterValues the values for the named placeholders in kernelTransaction
     * @return the list of {@link JsonObject}s retrieved by this query
     * @see N1qlQuery#parameterized(Statement, JsonObject)
     */
    public List<JsonObject> executeParametrizedStatement(String statement, List<String> parameterNames,
                                                         List<Object> parameterValues) {
        JsonObject namedParams = JsonObject.create();
        for (int param = 0; param < parameterNames.size(); param++) {
            namedParams.put(parameterNames.get(param), parameterValues.get(param));
        }
        ParameterizedN1qlQuery query = N1qlQuery.parameterized(statement, namedParams);
        return executeQuery(query);
    }

    private List<JsonObject> executeQuery(N1qlQuery query) {
        if(this.bucket.isClosed()){
            throw new RuntimeException("bucket has been closed before performing the query");
        }

        N1qlQueryResult queryResult = this.bucket.query(query);
        List<JsonObject> result = null;
        if (queryResult != null && queryResult.info().errorCount() == 0 && queryResult.info().resultCount() > 0) {
            result = new ArrayList<JsonObject>();
            for (N1qlQueryRow queryRow : queryResult) {
                result.add(queryRow.value());
            }
        }
        return result;
    }
}
