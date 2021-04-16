package apoc.couchbase;

import apoc.Extended;
import apoc.couchbase.document.CouchbaseByteArrayDocument;
import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import apoc.couchbase.document.CouchbaseUtils;
import apoc.result.BooleanResult;
import apoc.util.MissingDependencyException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.BinaryCollection;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.query.QueryOptions;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.stream.Stream;

/**
 * Neo4j Procedures for <b>Couchbase integration</b>.
 * <p/>
 * All of the operations performed against the Couchbase Server are done through
 * a {@link CouchbaseConnection}.
 * <br/>
 * A {@link CouchbaseConnection} can be created via the {@link CouchbaseManager}
 * .{@link CouchbaseManager#getConnection getConnection} method.
 * <p/>
 * Available operations are:
 * <ul>
 * <li>{@link #get} to retrieve a json document by its unique ID</li>
 * <li>{@link #exists} to check whether a json document with the given ID does exist</li>
 * <li>{@link #insert} to insert a json document if it does not exist already</li>
 * <li>{@link #upsert} to insert or overwrite a json document</li>
 * <li>{@link #remove} to remove the json document identified by its unique ID</li>
 * <li>{@link #replace} to replace the content of the json document identified by its unique ID</li>
 * <li>{@link #append} to append a json document's content to an existing one</li>
 * <li>{@link #prepend} to prepend a json document's content to an existing one</li>
 * </ul>
 * <p/>
 * N1QL query can be executed via the following methods:
 * <ul>
 * <li>{@link #query} for plain un-parameterized N1QL statements</li>
 * <li>{@link #posParamsQuery} for N1QL statements with positional parameters</li>
 * <li>{@link #namedParamsQuery} for N1QL statements with named parameters</li>
 * </ul>
 * For instance, after inserting a JSON document this way:
 * <p/>
 * <code>
 * call apoc.couchbase.insert(['localhost'], 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"]}')
 * </code>
 * <p/>
 * you can read the just inserted document via:
 * <p/>
 * <code>
 * call apoc.couchbase.query(['localhost'], 'default', 'select * from default') yield queryResult<br/>
 * unwind queryResult as queryResultRow<br/>
 * call apoc.convert.toMap(queryResultRow) yield value as queryResultRowMap<br/>
 * with queryResultRowMap.default as content<br/>
 * return content.firstName, content.secondName, content.lastName, content.notableWorks
 * </code>
 * <p/>
 * Using JSON fields instead of the &quot;*&quot; notation makes things a bit easier:
 * <p/>
 * <code>
 * call apoc.couchbase.query(['localhost'], 'default', 'select firstName, secondName, lastName, notableWorks from default') yield queryResult<br/>
 * unwind queryResult as queryRow<br/>
 * return queryRow.firstName, queryRow.secondName, queryRow.lastName, queryRow.notableWorks
 * </code>
 *
 * @author inserpio
 * @since 15.8.2016
 */
@Extended
public class Couchbase {

    /**
     * Retrieves a {@link GetResult} by its unique ID.
     * <p/>
     * Example:
     * <code>call apoc.couchbase.get('localhost', 'default', 'artist:vincent_van_gogh') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @return the found {@link CouchbaseJsonDocument} or null if not found
     * @see Collection#get(String)
     */
    @Procedure
    @Description("apoc.couchbase.get(hostOrKey, bucket, documentId) yield id, expiry, cas, mutationToken, content - retrieves a couchbase json document by its unique ID.")
    public Stream<CouchbaseJsonDocument> get(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                             @Name("documentId") String documentId) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            final GetResult getResult = couchbaseConnection.get(documentId);
            return getResult == null ? Stream.empty() : Stream.of(new CouchbaseJsonDocument(getResult, documentId));
        }
    }

    /**
     * Check whether a document with the given ID does exist.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.exists('localhost', 'default', 'artist:vincent_van_gogh') yield value</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @return true if it exists, false otherwise.
     * @see Collection#exists(String)
     */
    @Procedure
    @Description("apoc.couchbase.exists(hostOrKey, bucket, documentId) yield value - check whether a couchbase json document with the given ID does exist.")
    public Stream<BooleanResult> exists(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                        @Name("documentId") String documentId) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            return Stream.of(new BooleanResult(couchbaseConnection.exists(documentId)));
        }

    }

    /**
     * Insert a document if it does not exist already.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.insert('localhost', 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"]}') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to store
     * @return the newly created document
     * @see Collection#insert(String, Object)
     */
    @Procedure
    @Description("apoc.couchbase.insert(hostOrKey, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - insert a couchbase json document with its unique ID.")
    public Stream<CouchbaseJsonDocument> insert(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                                @Name("documentId") String documentId, @Name("json") String json) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            final MutationResult insert = couchbaseConnection.insert(documentId, json);
            GetResult getResult = couchbaseConnection.get(documentId);
            return Stream.of(new CouchbaseJsonDocument(getResult, documentId, insert.mutationToken().orElse(null)));
        }
    }

    /**
     * Insert or overwrite a document.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.upsert('localhost', 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"]}') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to store
     * @return the newly created or overwritten document or null in
     * case of exception
     * @see Collection#upsert(String, Object)
     */
    @Procedure
    @Description("apoc.couchbase.upsert(hostOrKey, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - insert or overwrite a couchbase json document with its unique ID.")
    public Stream<CouchbaseJsonDocument> upsert(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                                @Name("documentId") String documentId, @Name("json") String json) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            final MutationResult upsert = couchbaseConnection.upsert(documentId, json);
            GetResult getResult = couchbaseConnection.get(documentId);
            return Stream.of(new CouchbaseJsonDocument(getResult, documentId, upsert.mutationToken().orElse(null)));
        }
    }

    /**
     * Append a document's content to an existing one.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.append('localhost', 'default', 'artist:vincent_van_gogh', 'hello world'.getBytes()) yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param content    the byte[] representing the document to append
     * @return the updated document or null in case of exception
     * @see BinaryCollection#append(String, byte[])
     */
    @Procedure
    @Description("apoc.couchbase.append(hostOrKey, bucket, documentId, content) yield id, expiry, cas, mutationToken, content - append a couchbase json document to an existing one.")
    public Stream<CouchbaseByteArrayDocument> append(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                                     @Name("documentId") String documentId, @Name("content") byte[] content) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            final MutationResult append = couchbaseConnection.append(documentId, content);
            GetResult getResult = couchbaseConnection.getBinary(documentId);
            return Stream.of(new CouchbaseByteArrayDocument(getResult, documentId, append.mutationToken().orElse(null)));
        }
    }

    /**
     * Prepend a document's content to an existing one.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.prepend('localhost', 'default', 'artist:vincent_van_gogh', 'hello world'.getBytes()) yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param content    the byte[] representing the document to prepend
     * @return the updated document or null in case of exception
     * @see BinaryCollection#prepend(String, byte[])
     */
    @Procedure
    @Description("apoc.couchbase.prepend(hostOrKey, bucket, documentId, content) yield id, expiry, cas, mutationToken, content - prepend a couchbase json document to an existing one.")
    public Stream<CouchbaseByteArrayDocument> prepend(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                                      @Name("documentId") String documentId, @Name("content") byte[] content) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            final MutationResult prepend = couchbaseConnection.prepend(documentId, content);
            GetResult getResult = couchbaseConnection.getBinary(documentId);
            return Stream.of(new CouchbaseByteArrayDocument(getResult, documentId, prepend.mutationToken().orElse(null)));
        }
    }

    /**
     * Removes the document identified by its unique ID.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.remove(['localhost'], 'default', 'artist:vincent_van_gogh') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @return the removed document
     * @see Collection#remove(String)
     */
    @Procedure
    @Description("apoc.couchbase.remove(hostOrKey, bucket, documentId) yield id, expiry, cas, mutationToken, content - remove the couchbase json document identified by its unique ID.")
    public Stream<CouchbaseJsonDocument> remove(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                                @Name("documentId") String documentId) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            GetResult getResult = couchbaseConnection.get(documentId);
            final MutationResult remove = couchbaseConnection.remove(documentId);
            return Stream.of(new CouchbaseJsonDocument(getResult, documentId, remove.mutationToken().orElse(null)));
        }
    }

    /**
     * Replace the content of the document identified by its unique
     * ID.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.replace('localhost', 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"],"placeOfBirth":"Zundert","placeOfDeath":" Auvers-sur-Oise"}') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to prepend
     * @return the replaced document
     * @see Collection#replace(String, Object)
     */
    @Procedure
    @Description("apoc.couchbase.replace(hostOrKey, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - replace the content of the couchbase json document identified by its unique ID.")
    public Stream<CouchbaseJsonDocument> replace(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                                 @Name("documentId") String documentId, @Name("json") String json) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            final MutationResult replace = couchbaseConnection.replace(documentId, json);
            GetResult getResult = couchbaseConnection.get(documentId);
            return Stream.of(new CouchbaseJsonDocument(getResult, documentId, replace.mutationToken().orElse(null)));
        }
    }

    /**
     * Executes a plain un-parameterized N1QL statement.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.query('localhost', 'default', 'select * from default where lastName = "Van Gogh"']) yield queryResult</code>
     *
     * @param hostOrKey a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket    the bucket to open; if null is passed then it's used the "default"
     *                  bucket
     * @param statement the raw statement string to execute
     * @return the list of {@link JsonObject}s retrieved by this query in the form
     * of a {@link CouchbaseQueryResult}
     * @see Cluster#query(String)
     */
    @Procedure
    @Description("apoc.couchbase.query(hostOrKey, bucket, statement) yield queryResult - executes a plain un-parameterized N1QL statement.")
    public Stream<CouchbaseQueryResult> query(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                              @Name("statement") String statement) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            List<JsonObject> statementResult = couchbaseConnection.executeStatement(statement);
            final CouchbaseQueryResult result = CouchbaseUtils.convertToCouchbaseQueryResult(statementResult);
            return result == null ? Stream.empty() : Stream.of(result);
        }
    }

    /**
     * Executes a N1QL statement with positional parameters.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.posParamsQuery('localhost', 'default', 'select * from default where lastName = $1', ['Van Gogh']) yield queryResult</code>
     *
     * @param hostOrKey a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket    the bucket to open; if null is passed then it's used the "default"
     *                  bucket
     * @param statement the raw statement string to execute (containing positional
     *                  placeholders: $1, $2, ...)
     * @param params    the values for the positional placeholders in statement
     * @return the list of {@link JsonObject}s retrieved by this query in the form
     * of a {@link CouchbaseQueryResult}
     * @see Cluster#query(String, QueryOptions)
     */
    @Procedure
    @Description("apoc.couchbase.posParamsQuery(hostOrKey, bucket, statement, params) yield queryResult - executes a N1QL statement with positional parameters.")
    public Stream<CouchbaseQueryResult> posParamsQuery(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                                       @Name("statement") String statement, @Name("params") List<Object> params) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            List<JsonObject> statementResult = couchbaseConnection.executeParameterizedStatement(statement, params);
            final CouchbaseQueryResult result = CouchbaseUtils.convertToCouchbaseQueryResult(statementResult);
            return result == null ? Stream.empty() : Stream.of(result);
        }
    }

    /**
     * Executes a N1QL statement with named parameters.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.namedParamsQuery('localhost', 'default', 'select * from default where lastName = $lastName', ['lastName'], ['Van Gogh']) yield queryResult</code>
     *
     * @param hostOrKey   a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket      the bucket to open; if null is passed then it's used the "default"
     *                    bucket
     * @param statement   the raw statement string to execute (containing named
     *                    placeholders: $param1, $param2, ...)
     * @param paramNames  the placeholders' names in statement
     * @param paramValues the values for the named placeholders in statement
     * @return the list of {@link JsonObject}s retrieved by this query in the form
     * of a {@link CouchbaseQueryResult}
     * @see Cluster#query(String, QueryOptions)
     */
    @Procedure
    @Description("apoc.couchbase.namedParamsQuery(hostkOrKey, bucket, statement, paramNames, paramValues) yield queryResult - executes a N1QL statement with named parameters.")
    public Stream<CouchbaseQueryResult> namedParamsQuery(@Name("hostOrKey") String hostOrKey, @Name("bucket") String bucket,
                                                         @Name("statement") String statement, @Name("paramNames") List<String> paramNames,
                                                         @Name("paramValues") List<Object> paramValues) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket)) {
            List<JsonObject> statementResult = couchbaseConnection.executeParameterizedStatement(statement, paramNames,
                    paramValues);
            final CouchbaseQueryResult result = CouchbaseUtils.convertToCouchbaseQueryResult(statementResult);
            return result == null ? Stream.empty() : Stream.of(result);
        }
    }

    private CouchbaseConnection getCouchbaseConnection(String hostOrKey, String bucket) {
        try {
            return CouchbaseManager.getConnection(hostOrKey, bucket);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException("Cannot find the jar into the plugins folder. \n" +
                    "Please put these jar in the plugins folder : \n\n" +
                    "java-client-x.y.z.jar\n" +
                    "\n" +
                    "core-io-x.y.z.jar\n" +
                    "\n" +
                    "rxjava-x.y.z.jar\n" +
                    "\n" +
                    "See the documentation: https://neo4j-contrib.github.io/neo4j-apoc-procedures/#_interacting_with_couchbase");
        }
    }
}
