package apoc.couchbase.document;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;

/**
 * Transports the list of {@link JsonObject}s retrieved by a N1QL query so that
 * it can be {@link Stream}-ed and returned by the procedures
 * <p/>
 * Every {@link JsonObject}s retrieved by a N1QL query is first converted into a
 * {@link Map Map&lt;String, Object&gt;} and then added to the embedded
 * {@link #queryResult} list.
 * 
 * @since 15.8.2016
 * @author inserpio
 * 
 * @see CouchbaseUtils#convertToCouchbaseQueryResult(List)
 * @see QueryResult
 */
public class CouchbaseQueryResult {

  public List<Map<String, Object>> queryResult;

  public CouchbaseQueryResult() {
    this.queryResult = null;
  }

  public CouchbaseQueryResult(List<Map<String, Object>> value) {
    this.queryResult = value;
  }
}
