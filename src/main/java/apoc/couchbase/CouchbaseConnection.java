package apoc.couchbase;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseAsyncCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.ParameterizedN1qlQuery;
import com.couchbase.client.java.query.SimpleN1qlQuery;
import com.couchbase.client.java.query.Statement;

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
 * @since 15.8.2016
 * @author inserpio
 */
public class CouchbaseConnection implements AutoCloseable {

  private Cluster cluster;
  private Bucket bucket;

  /**
   * Opens a {@link Bucket} identified by its name with an empty password and
   * with the default connect timeout.
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucketName
   *          the name of the bucket to open; if null is passed then it's used
   *          the "default" bucket name
   */
  protected CouchbaseConnection(Cluster cluster, String bucketName) {
    this.cluster = cluster;
    if (bucketName == null) {
      bucketName = CouchbaseAsyncCluster.DEFAULT_BUCKET;
    }
    this.bucket = cluster.openBucket(bucketName);
  }

  /**
   * Disconnect and close all buckets.
   * 
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() {
    this.cluster.disconnect();
  }

  /**
   * Retrieves a {@link JsonDocument} by its unique ID.
   * 
   * @param documentId
   *          the unique ID of the document
   * @return the found {@link JsonDocument} or null if not found
   * 
   * @see Bucket#get(String)
   */
  public JsonDocument get(String documentId) {
    return this.bucket.get(documentId);
  }

  /**
   * Checks whether a {@link JsonDocument} with the given ID does exist.
   * 
   * @param documentId
   *          the unique ID of the document
   * @return true if it exists, false otherwise.
   * 
   * @see Bucket#exists(String)
   */
  public boolean exists(String documentId) {
    return this.bucket.exists(documentId);
  }

  /**
   * Inserts a {@link JsonDocument} if it does not exist already.
   * 
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to store
   * @return the newly created {@link JsonDocument}
   * 
   * @see Bucket#insert(Document)
   */
  public JsonDocument insert(String documentId, String json) {
    return this.bucket.insert(JsonDocument.create(documentId, JsonObject.fromJson(json)));
  }

  /**
   * Inserts or overwrites a {@link JsonDocument}.
   * 
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to store
   * @return the newly created or overwritten {@link JsonDocument}
   * 
   * @see Bucket#upsert(Document)
   */
  public JsonDocument upsert(String documentId, String json) {
    return this.bucket.upsert(JsonDocument.create(documentId, JsonObject.fromJson(json)));
  }

  /**
   * Appends a {@link JsonDocument}'s content to an existing one.
   * 
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to append
   * @return the updated {@link Document}
   * 
   * @see Bucket#append(Document)
   */
  public JsonDocument append(String documentId, String json) {
    this.bucket.append(JsonDocument.create(documentId, JsonObject.fromJson(json)));
    return this.get(documentId);
  }

  /**
   * Prepends a {@link JsonDocument}'s content to an existing one.
   * 
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to prepend
   * @return the updated {@link Document}
   * 
   * @see Bucket#prepend(Document)
   */
  public JsonDocument prepend(String documentId, String json) {
    this.bucket.prepend(JsonDocument.create(documentId, JsonObject.fromJson(json)));
    return this.get(documentId);
  }

  /**
   * Removes the {@link JsonDocument} identified by its unique ID.
   * 
   * @param documentId
   *          the unique ID of the document
   * @return the removed {@link JsonDocument}
   * 
   * @see Bucket#remove(String)
   */
  public JsonDocument remove(String documentId) {
    return this.bucket.remove(documentId);
  }

  /**
   * Replaces a {@link JsonDocument} if it does already exist.
   * 
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to replace
   * @return the replaced {@link Document}
   * 
   * @see Bucket#replace(Document)
   */
  public JsonDocument replace(String documentId, String json) {
    return this.bucket.replace(JsonDocument.create(documentId, JsonObject.fromJson(json)));
  }

  /**
   * Executes a plain un-parameterized N1QL statement.
   * 
   * @param statement
   *          the raw statement string to execute
   * @return the list of {@link JsonObject}s retrieved by this query
   * 
   * @see N1qlQuery#simple(Statement)
   */
  public List<JsonObject> executeStatement(String statement) {
    SimpleN1qlQuery query = N1qlQuery.simple(statement);
    return executeQuery(query);
  }

  /**
   * Executes a N1QL statement with positional parameters.
   * 
   * @param statement
   *          the raw statement string to execute (containing positional
   *          placeholders: $1, $2, ...)
   * @param parameters
   *          the values for the positional placeholders in statement
   * @return the list of {@link JsonObject}s retrieved by this query
   * 
   * @see N1qlQuery#parameterized(Statement, JsonArray)
   */
  public List<JsonObject> executeParametrizedStatement(String statement, List<Object> parameters) {
    JsonArray positionalParams = JsonArray.from(parameters);
    ParameterizedN1qlQuery query = N1qlQuery.parameterized(statement, positionalParams);
    return executeQuery(query);
  }

  /**
   * Executes a N1QL statement with named parameters.
   * 
   * @param statement
   *          the raw statement string to execute (containing named
   *          placeholders: $param1, $param2, ...)
   * @param parameterNames
   *          the placeholders' names in statement
   * @param parameterValues
   *          the values for the named placeholders in statement
   * @return the list of {@link JsonObject}s retrieved by this query
   * 
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
