package apoc.couchbase;

import apoc.couchbase.document.CouchbaseByteArrayDocument;
import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import apoc.couchbase.document.CouchbaseUtils;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.BinaryCollection;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import org.apache.commons.configuration2.Configuration;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.couchbase.CouchbaseManager.PORT_CONFIG_KEY;
import static apoc.couchbase.CouchbaseManager.URI_CONFIG_KEY;
import static apoc.couchbase.CouchbaseManager.checkAndGetURI;
import static apoc.couchbase.CouchbaseManager.getKeyMap;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

/**
 * A Couchbase Server connection.
 * <p/>
 * It wraps a Couchbase {@link Bucket} instance through that all the
 * operations performed against Couchbase are done,
 * and return a result digestible by Neo4j.
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

    private static final String SPLIT_URI_KEY = ",";

    private Cluster cluster;
    private Bucket bucket;
    private Collection collection;
    private BinaryCollection binaryCollection;

    private ClusterEnvironment env;

    /**
     * @param hostOrKey
     * @param authenticator
     * @param bucketName
     */
    protected CouchbaseConnection(String hostOrKey, PasswordAuthenticator authenticator, String bucketName, CouchbaseConfig config) {

        // get Set<SeedNode> by hostOrKey
        Set<SeedNode> seedNodes;
        URI singleHostURI = checkAndGetURI(hostOrKey);
        String url;
        if (singleHostURI == null || singleHostURI.getScheme() == null) {

            Configuration couchbaseConfig = getKeyMap(hostOrKey);
            int port = couchbaseConfig.getInt(PORT_CONFIG_KEY, -1);
            url = couchbaseConfig.getString(URI_CONFIG_KEY, null);
            if (url == null) {
                throw new RuntimeException("Please check you 'apoc.couchbase." + hostOrKey + "' configuration, url is missing");
            }

            // I can type apoc.couchbase.mykey.uri=host1,host2,host3
            List<String> splitUrl = Arrays.asList(url.split(SPLIT_URI_KEY));

            seedNodes = splitUrl.stream().map(singleUri ->
                    SeedNode.create(singleUri,
                            Optional.empty(),
                            port != -1 ? Optional.of(port) : Optional.empty()))
                    .collect(Collectors.toSet());
        } else {
            url = singleHostURI.getHost();
            final int port = singleHostURI.getPort();
            seedNodes = Set.of(SeedNode.create(url,
                    Optional.empty(),
                    port != -1 ? Optional.of(port) : Optional.empty()
            ));
        }

        this.env = config.getEnv();
        this.cluster = Cluster.connect(seedNodes, clusterOptions(authenticator).environment(env));
        try {
            this.bucket = this.cluster.bucket(bucketName);
            if (config.getWaitUntilReady() != null) {
                this.bucket.waitUntilReady(Duration.ofMillis(config.getWaitUntilReady()));
            }
            this.collection = this.bucket.scope(config.getScope()).collection(config.getCollection());
            this.binaryCollection = this.collection.binary();
        } catch (Exception e) {
            this.close();
            throw new RuntimeException(e);
        }
    }

    /**
     * Disconnect the cluster and shuts down the environment
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() {
        try {
            this.cluster.disconnect();
        } catch (Exception ignored) {}
        try {
            this.env.shutdown();
        } catch (Exception ignored) {}
    }

    public Collection getCollection() {
        return collection;
    }

    /**
     * Retrieves a {@link GetResult} by its unique ID.
     * 
     * @param documentId the unique ID of the document
     * @return the found {@link CouchbaseJsonDocument} or null if not found
     */
    public CouchbaseJsonDocument get(String documentId) {
        GetResult getResult = getResult(documentId);
        if (getResult == null) {
            return null;
        }
        return new CouchbaseJsonDocument(getResult, documentId);
    }

    /**
     * Retrieves a {@link GetResult} by its unique ID.
     *
     * @param documentId the unique ID of the document
     * @return the found {@link GetResult} or null if not found
     * @see Collection#get(String)
     */
    private GetResult getResult(String documentId) {
        try {
            return collection.get(documentId, getOptions().withExpiry(true));
        } catch (DocumentNotFoundException e) {
            return null;
        }
    }

    public GetResult getBinary(String documentId) {
        try {
            return collection.get(documentId, getOptions().transcoder(RawBinaryTranscoder.INSTANCE).withExpiry(true));
        } catch (DocumentNotFoundException e) {
            return null;
        }
    }

    /**
     * Checks whether a document with the given ID does exist.
     *
     * @param documentId the unique ID of the document
     * @return true if it exists, false otherwise.
     * @see Collection#exists(String)
     */
    public boolean exists(String documentId) {
        return this.collection.exists(documentId).exists();
    }

    /**
     * Inserts a full document which does not exist yet.
     *
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to store
     * @return the newly created {@link MutationResult}
     * @see Collection#insert(String, Object)
     */
    public CouchbaseJsonDocument insert(String documentId, String json) {
        MutationResult insert = this.collection.insert(documentId, JsonObject.fromJson(json));
        GetResult getResult = getResult(documentId);
        return new CouchbaseJsonDocument(getResult, documentId, insert.mutationToken().orElse(null));
    }

    /**
     * Inserts or overwrites a document.
     *
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to store
     * @return the newly created or overwritten {@link MutationResult}
     * @see Collection#upsert(String, Object)
     */
    public CouchbaseJsonDocument upsert(String documentId, String json) {
        MutationResult upsertResult = this.collection.upsert(documentId, JsonObject.fromJson(json));
        GetResult getResult = getResult(documentId);
        return new CouchbaseJsonDocument(getResult, documentId, upsertResult.mutationToken().orElse(null));
    }

    /**
     * Appends a json content to an existing one.
     *
     * @param documentId the unique ID of the document
     * @param content    the byte[] representing the document to append
     * @return the updated {@link MutationResult}
     * @see BinaryCollection#append(String, byte[])
     */
    public CouchbaseByteArrayDocument append(String documentId, byte[] content) {
        MutationResult appendResult = binaryCollection.append(documentId, content);
        GetResult getResult = getBinary(documentId);
        return new CouchbaseByteArrayDocument(getResult, documentId, appendResult.mutationToken().orElse(null));
    }

    /**
     * Prepends a json content to an existing one.
     *
     * @param documentId the unique ID of the document
     * @param content       the byte[] representing the document to prepend
     * @return the updated {@link MutationResult}
     * @see BinaryCollection#prepend(String, byte[])
     */
    public CouchbaseByteArrayDocument prepend(String documentId, byte[] content) {
        MutationResult prependResult = binaryCollection.prepend(documentId, content);
        GetResult binaryResult = getBinary(documentId);
        return new CouchbaseByteArrayDocument(binaryResult, documentId, prependResult.mutationToken().orElse(null));
    }

    /**
     * Removes the document identified by its unique ID.
     *
     * @param documentId the unique ID of the document
     * @return the removed document
     * @see Collection#remove(String)
     */
    public CouchbaseJsonDocument remove(String documentId) {
        GetResult getResult = getResult(documentId);
        MutationResult removeResult = this.collection.remove(documentId);
        return new CouchbaseJsonDocument(getResult, documentId, removeResult.mutationToken().orElse(null));
    }

    /**
     * Replaces a document if it does already exist.
     *
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to replace
     * @return the replaced {@link MutationResult}
     * @see Collection#replace(String, Object)
     */
    public CouchbaseJsonDocument replace(String documentId, String json) {
        MutationResult replaceResult = this.collection.replace(documentId, JsonObject.fromJson(json));
        GetResult getResult = getResult(documentId);
        return new CouchbaseJsonDocument(getResult, documentId, replaceResult.mutationToken().orElse(null));
    }

    /**
     * Executes a plain un-parameterized N1QL kernelTransaction.
     *
     * @param statement the raw kernelTransaction string to execute
     * @return the list of {@link JsonObject}s retrieved by this query
     * @see Cluster#query(String)
     */
    public CouchbaseQueryResult executeStatement(String statement) {
        List<JsonObject> statementResult = this.cluster.query(statement).rowsAsObject();
        return CouchbaseUtils.convertToCouchbaseQueryResult(statementResult);
    }

    /**
     * Executes a N1QL kernelTransaction with positional parameters.
     *
     * @param statement  the raw kernelTransaction string to execute (containing positional
     *                   placeholders: $1, $2, ...)
     * @param parameters the values for the positional placeholders in kernelTransaction
     * @return the list of {@link JsonObject}s retrieved by this query
     * @see Cluster#query(String, QueryOptions)
     */
    public CouchbaseQueryResult executeParameterizedStatement(String statement, List<Object> parameters) {
        JsonArray positionalParams = JsonArray.from(parameters);
        final QueryResult queryResult = this.cluster.query(statement, queryOptions().parameters(positionalParams));
        List<JsonObject> statementResult = queryResult.rowsAsObject();
        return CouchbaseUtils.convertToCouchbaseQueryResult(statementResult);
    }

    /**
     * Executes a N1QL kernelTransaction with named parameters.
     *
     * @param statement       the raw kernelTransaction string to execute (containing named
     *                        placeholders: $param1, $param2, ...)
     * @param parameterNames  the placeholders' names in kernelTransaction
     * @param parameterValues the values for the named placeholders in kernelTransaction
     * @return the list of {@link JsonObject}s retrieved by this query
     * @see Cluster#query(String, QueryOptions)
     */
    public CouchbaseQueryResult executeParameterizedStatement(String statement, List<String> parameterNames,
                                                          List<Object> parameterValues) {
        JsonObject namedParams = JsonObject.create();
        for (int param = 0; param < parameterNames.size(); param++) {
            namedParams.put(parameterNames.get(param), parameterValues.get(param));
        }
        QueryResult queryResult = this.cluster.query(statement, queryOptions().parameters(namedParams));
        List<JsonObject> statementResult = queryResult.rowsAsObject();
        return CouchbaseUtils.convertToCouchbaseQueryResult(statementResult);
    }
}