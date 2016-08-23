package apoc.couchbase.document;

import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Represents a {@link Document} that contains a <code>JSON object</code> as the
 * content.
 * <p/>
 * The <code>JSON object</code> here comes in the form of a <code>Map</code>.
 * 
 * @since 15.8.2016
 * @author inserpio
 * @see JsonDocument
 * @see JsonObject
 */
public class CouchbaseJsonDocument implements CouchbaseDocument<Map<String, Object>> {

  /**
   * The per-bucket unique ID of the {@link Document}.
   */
  public String id;

  /**
   * The optional expiration time for the {@link Document} (0 if not set).
   */
  public long expiry;

  /**
   * The last-known CAS (<i>Compare And Swap</i>) value for the {@link Document} (0 if not set).
   */
  public long cas;

  /**
   * The optional, opaque mutation token set after a successful mutation and if
   * enabled on the environment.
   */
  public Map<String, Object> mutationToken;

  /**
   * The content of the {@link Document}.
   */
  public Map<String, Object> content;

  public CouchbaseJsonDocument(JsonDocument jsonDocument) {
    this.id = jsonDocument.id();
    this.expiry = jsonDocument.expiry();
    this.cas = jsonDocument.cas();
    this.mutationToken = CouchbaseUtils.convertMutationTokenToMap(jsonDocument.mutationToken());
    this.content = (jsonDocument.content() != null) ? this.content = jsonDocument.content().toMap()
        : new HashMap<String, Object>();
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
