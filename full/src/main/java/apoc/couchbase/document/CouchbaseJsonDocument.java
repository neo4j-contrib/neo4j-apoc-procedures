package apoc.couchbase.document;

import java.time.Instant;
import java.util.Map;

import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;

import static apoc.couchbase.document.CouchbaseUtils.convertMutationTokenToMap;

/**
 * Represents a {@link MutationResult} or a {@link GetResult} (in case of apoc.couchbase.get procedure)
 * that contains a <code>JSON object</code> as the
 * content.
 * <p/>
 * The <code>JSON object</code> here comes in the form of a <code>Map</code>.
 * 
 * @since 15.8.2016
 * @author inserpio
 * @see JsonObject
 */
public class CouchbaseJsonDocument implements CouchbaseDocument<Object> {

  /**
   * The per-bucket unique ID of the {@link GetResult} or the {@link MutationResult}.
   */
  public final String id;

  /**
   * The optional expiration time for the {@link GetResult} (0 if not set).
   */
  public final long expiry;

  /**
   * The last-known CAS (<i>Compare And Swap</i>) value for the {@link MutationResult} (0 if not set).
   */
  public final long cas;

  /**
   * The optional, opaque mutation token set after a successful mutation and if
   * enabled on the environment.
   */
  public final Map<String, Object> mutationToken;

  /**
   * The content of the {@link GetResult}.
   */
  public Object content;

  private CouchbaseJsonDocument(String id, long expiry, long cas, Map<String, Object> mutationToken, Object content) {
    this.id = id;
    this.expiry = expiry;
    this.cas = cas;
    this.mutationToken = mutationToken;
    this.content = content;
  }

  public CouchbaseJsonDocument(GetResult getResult, String id) {
    this(getResult, id, null);
  }

  public CouchbaseJsonDocument(GetResult getResult, String id, MutationToken mutationToken, boolean isBinary) {
    this(id, getResult.expiryTime().orElse(Instant.ofEpochMilli(0)).toEpochMilli(),
            getResult.cas(), convertMutationTokenToMap(mutationToken),
            isBinary ? getResult.contentAs(byte[].class) : getResult.contentAsObject().toMap());
  }

  public CouchbaseJsonDocument(GetResult getResult, String id, MutationToken mutationToken) {
    this(getResult, id, mutationToken, false);
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public Object getContent() {
    return this.content;
  }

  @Override
  public long getCas() {
    return this.cas;
  }

  @Override
  public long getExpiry() {
    return this.expiry;
  }

  @Override
  public Map<String, Object> getMutationToken() {
    return this.mutationToken;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(this.getClass().getSimpleName() + " {");
    sb.append("id='").append(getId()).append('\'');
    sb.append(", cas=").append(getCas());
    sb.append(", expiry=").append(getExpiry());
    sb.append(", content=").append(getContent());
    sb.append(", mutationToken=").append(getMutationToken());
    sb.append('}');
    return sb.toString();
  }
}
