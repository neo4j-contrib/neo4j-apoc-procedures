package apoc.couchbase;

import java.util.List;
import java.util.stream.Stream;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Statement;

import org.neo4j.procedure.Description;
import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import apoc.couchbase.document.CouchbaseUtils;
import apoc.result.BooleanResult;

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
 * @since 15.8.2016
 * @author inserpio
 */
public class Couchbase {

  /**
   * Retrieves a {@link JsonDocument} by its unique ID.
   * <p/>
   * Example:
   * <code>call apoc.couchbase.get(['localhost'], 'default', 'artist:vincent_van_gogh') yield id, expiry, cas, mutationToken, content</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param documentId
   *          the unique ID of the document
   * @return the found {@link CouchbaseJsonDocument} or null if not found
   * 
   * @see Bucket#get(String)
   */
  @Procedure
  @Description("apoc.couchbase.get(nodes, bucket, documentId) yield id, expiry, cas, mutationToken, content - retrieves a couchbase json document by its unique ID.")
  public Stream<CouchbaseJsonDocument> get(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("documentId") String documentId) {
    Stream<CouchbaseJsonDocument> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      JsonDocument jsonDocument = couchbaseConnection.get(documentId);
      if (jsonDocument != null) {
        result = Stream.of(new CouchbaseJsonDocument(jsonDocument));
      }
    }
    return result;
  }

  /**
   * Check whether a {@link JsonDocument} with the given ID does exist.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.exists(['localhost'], 'default', 'artist:vincent_van_gogh') yield value</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param documentId
   *          the unique ID of the document
   * @return true if it exists, false otherwise.
   * 
   * @see Bucket#exists(String)
   */
  @Procedure
  @Description("apoc.couchbase.exists(nodes, bucket, documentId) yield value - check whether a couchbase json document with the given ID does exist.")
  public Stream<BooleanResult> exists(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("documentId") String documentId) {
    Stream<BooleanResult> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      result = Stream.of(new BooleanResult(couchbaseConnection.exists(documentId)));
    }
    return result;
  }

  /**
   * Insert a {@link JsonDocument} if it does not exist already.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.insert(['localhost'], 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"]}') yield id, expiry, cas, mutationToken, content</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to store
   * @return the newly created {@link JsonDocument} or null in case of exception
   * 
   * @see Bucket#insert(Document)
   */
  @Procedure
  @Description("apoc.couchbase.insert(nodes, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - insert a couchbase json document with its unique ID.")
  public Stream<CouchbaseJsonDocument> insert(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("documentId") String documentId, @Name("json") String json) {
    Stream<CouchbaseJsonDocument> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      JsonDocument jsonDocument = couchbaseConnection.insert(documentId, json);
      if (jsonDocument != null) {
        result = Stream.of(new CouchbaseJsonDocument(jsonDocument));
      }
    }
    return result;
  }

  /**
   * Insert or overwrite a {@link JsonDocument}.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.upsert(['localhost'], 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"]}') yield id, expiry, cas, mutationToken, content</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to store
   * @return the newly created or overwritten {@link JsonDocument} or null in
   *         case of exception
   * 
   * @see Bucket#upsert(Document)
   */
  @Procedure
  @Description("apoc.couchbase.upsert(nodes, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - insert or overwrite a couchbase json document with its unique ID.")
  public Stream<CouchbaseJsonDocument> upsert(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("documentId") String documentId, @Name("json") String json) {
    Stream<CouchbaseJsonDocument> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      JsonDocument jsonDocument = couchbaseConnection.upsert(documentId, json);
      if (jsonDocument != null) {
        result = Stream.of(new CouchbaseJsonDocument(jsonDocument));
      }
    }
    return result;
  }

  /**
   * Append a {@link JsonDocument}'s content to an existing one.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.append(['localhost'], 'default', 'artist:vincent_van_gogh', '{"placeOfBirth":"Zundert","placeOfDeath":" Auvers-sur-Oise"}') yield id, expiry, cas, mutationToken, content</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to append
   * @return the updated {@link JsonDocument} or null in case of exception
   * 
   * @see Bucket#append(Document)
   */
  @Procedure
  @Description("apoc.couchbase.append(nodes, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - append a couchbase json document to an existing one.")
  public Stream<CouchbaseJsonDocument> append(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("documentId") String documentId, @Name("json") String json) {
    Stream<CouchbaseJsonDocument> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      JsonDocument jsonDocument = couchbaseConnection.append(documentId, json);
      if (jsonDocument != null) {
        result = Stream.of(new CouchbaseJsonDocument(jsonDocument));
      }
    }
    return result;
  }

  /**
   * Prepend a {@link JsonDocument}'s content to an existing one.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.prepend(['localhost'], 'default', 'artist:vincent_van_gogh', '{"placeOfBirth":"Zundert","placeOfDeath":" Auvers-sur-Oise"}') yield id, expiry, cas, mutationToken, content</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to prepend
   * @return the updated {@link JsonDocument} or null in case of exception
   * 
   * @see Bucket#prepend(Document)
   */
  @Procedure
  @Description("apoc.couchbase.prepend(nodes, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - prepend a couchbase json document to an existing one.")
  public Stream<CouchbaseJsonDocument> prepend(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("documentId") String documentId, @Name("json") String json) {
    Stream<CouchbaseJsonDocument> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      JsonDocument jsonDocument = couchbaseConnection.prepend(documentId, json);
      if (jsonDocument != null) {
        result = Stream.of(new CouchbaseJsonDocument(jsonDocument));
      }
    }
    return result;
  }

  /**
   * Removes the {@link JsonDocument} identified by its unique ID.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.remove(['localhost'], 'default', 'artist:vincent_van_gogh') yield id, expiry, cas, mutationToken, content</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param documentId
   *          the unique ID of the document
   * @return the removed {@link JsonDocument}
   * 
   * @see Bucket#remove(String)
   */
  @Procedure
  @Description("apoc.couchbase.remove(nodes, bucket, documentId) yield id, expiry, cas, mutationToken, content - remove the couchbase json document identified by its unique ID.")
  public Stream<CouchbaseJsonDocument> remove(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("documentId") String documentId) {
    Stream<CouchbaseJsonDocument> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      JsonDocument jsonDocument = couchbaseConnection.remove(documentId);
      if (jsonDocument != null) {
        result = Stream.of(new CouchbaseJsonDocument(jsonDocument));
      }
    }
    return result;
  }

  /**
   * Replace the content of the {@link JsonDocument} identified by its unique
   * ID.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.replace(['localhost'], 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"],"placeOfBirth":"Zundert","placeOfDeath":" Auvers-sur-Oise"}') yield id, expiry, cas, mutationToken, content</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param documentId
   *          the unique ID of the document
   * @param json
   *          the JSON String representing the document to prepend
   * @return the replaced {@link JsonDocument}
   * 
   * @see Bucket#replace(Document)
   */
  @Procedure
  @Description("apoc.couchbase.replace(nodes, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - replace the content of the couchbase json document identified by its unique ID.")
  public Stream<CouchbaseJsonDocument> replace(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("documentId") String documentId, @Name("json") String json) {
    Stream<CouchbaseJsonDocument> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      JsonDocument jsonDocument = couchbaseConnection.replace(documentId, json);
      if (jsonDocument != null) {
        result = Stream.of(new CouchbaseJsonDocument(jsonDocument));
      }
    }
    return result;
  }

  /**
   * Executes a plain un-parameterized N1QL statement.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.query(['localhost'], 'default', 'select * from default where lastName = "Van Gogh"']) yield queryResult</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param statement
   *          the raw statement string to execute
   * @return the list of {@link JsonObject}s retrieved by this query in the form
   *         of a {@link CouchbaseQueryResult}
   * 
   * @see N1qlQuery#simple(Statement)
   */
  @Procedure
  @Description("apoc.couchbase.query(nodes, bucket, statement) yield queryResult - executes a plain un-parameterized N1QL statement.")
  public Stream<CouchbaseQueryResult> query(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("statement") String statement) {
    Stream<CouchbaseQueryResult> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      List<JsonObject> statementResult = couchbaseConnection.executeStatement(statement);
      if (statementResult != null) {
        result = Stream.of(CouchbaseUtils.convertToCouchbaseQueryResult(statementResult));
      }
    }
    return result;
  }

  /**
   * Executes a N1QL statement with positional parameters.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.posParamsQuery(['localhost'], 'default', 'select * from default where lastName = $1', ['Van Gogh']) yield queryResult</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param statement
   *          the raw statement string to execute (containing positional
   *          placeholders: $1, $2, ...)
   * @param params
   *          the values for the positional placeholders in statement
   * @return the list of {@link JsonObject}s retrieved by this query in the form
   *         of a {@link CouchbaseQueryResult}
   * 
   * @see N1qlQuery#parameterized(Statement, JsonArray)
   */
  @Procedure
  @Description("apoc.couchbase.posParamsQuery(nodes, bucket, statement, params) yield queryResult - executes a N1QL statement with positional parameters.")
  public Stream<CouchbaseQueryResult> posParamsQuery(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("statement") String statement, @Name("params") List<Object> params) {
    Stream<CouchbaseQueryResult> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      List<JsonObject> statementResult = couchbaseConnection.executeParametrizedStatement(statement, params);
      if (statementResult != null) {
        result = Stream.of(CouchbaseUtils.convertToCouchbaseQueryResult(statementResult));
      }
    }
    return result;
  }

  /**
   * Executes a N1QL statement with named parameters.
   * <p/>
   * Example:
   * <code>CALL apoc.couchbase.namedParamsQuery(['localhost'], 'default', 'select * from default where lastName = $lastName', ['lastName'], ['Van Gogh']) yield queryResult</code>
   * 
   * @param nodes
   *          the list of nodes to use when connecting to the cluster reference
   * @param bucket
   *          the bucket to open; if null is passed then it's used the "default"
   *          bucket
   * @param statement
   *          the raw statement string to execute (containing named
   *          placeholders: $param1, $param2, ...)
   * @param paramNames
   *          the placeholders' names in statement
   * @param paramValues
   *          the values for the named placeholders in statement
   * @return the list of {@link JsonObject}s retrieved by this query in the form
   *         of a {@link CouchbaseQueryResult}
   * 
   * @see N1qlQuery#parameterized(Statement, JsonObject)
   */
  @Procedure
  @Description("apoc.couchbase.namedParamsQuery(nodes, bucket, statement, paramNames, paramValues) yield queryResult - executes a N1QL statement with named parameters.")
  public Stream<CouchbaseQueryResult> namedParamsQuery(@Name("nodes") List<String> nodes, @Name("bucket") String bucket,
      @Name("statement") String statement, @Name("paramNames") List<String> paramNames,
      @Name("paramValues") List<Object> paramValues) {
    Stream<CouchbaseQueryResult> result = null;
    try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(nodes, bucket)) {
      List<JsonObject> statementResult = couchbaseConnection.executeParametrizedStatement(statement, paramNames,
          paramValues);
      if (statementResult != null) {
        result = Stream.of(CouchbaseUtils.convertToCouchbaseQueryResult(statementResult));
      }
    }
    return result;
  }
}
