package apoc.couchbase.document;

import java.time.Instant;
import java.util.Map;

import com.couchbase.client.java.Collection;
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
public class CouchbaseJsonDocument implements CouchbaseDocument<Map<String, Object>> {

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
  public final Map<String, Object> content;

  public CouchbaseJsonDocument(GetResult getResult, String id) {
    this.id = id;
    this.expiry = getExpiry(getResult);
    this.cas = getResult.cas();
    this.mutationToken = null;
    this.content = getResult.contentAsObject().toMap();
  }

  public CouchbaseJsonDocument(MutationResult mutationResult, String id, Collection collection) {

    GetResult getResult = collection.exists(id).exists() ? collection.get(id) : null;

    this.id = id;
    this.cas = mutationResult.cas();
    if (getResult == null) {
      this.expiry = 0;
      this.mutationToken = null;
      this.content = null;
    } else {
      this.expiry = getResult.expiryTime().orElse(Instant.ofEpochMilli(0)).toEpochMilli();
      this.mutationToken = convertMutationTokenToMap(mutationResult.mutationToken().orElse(null));
      this.content = getResult.contentAsObject().toMap();
    }
  }

  private long getExpiry(GetResult getResult) {
    return getResult.expiryTime().orElse(Instant.ofEpochMilli(0)).toEpochMilli();
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public Map<String, Object> getContent() {
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
