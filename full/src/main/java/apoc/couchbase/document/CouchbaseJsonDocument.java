package apoc.couchbase.document;

import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;

import java.util.Map;

/**
 * Represents a {@link MutationResult} or a {@link GetResult} (in case of apoc.couchbase.get procedure)
 * that contains a <code>JSON object</code> as the
 * content.
 * <p/>
 * The <code>JSON object</code> here comes in the form of a <code>Map</code>.
 *
 * @see JsonObject
 */
public class CouchbaseJsonDocument extends CouchbaseObjectDocument<Map<String, Object>> {

    /**
     * The json content of the {@link GetResult}.
     */
    public Map<String, Object> content;

    public CouchbaseJsonDocument(GetResult getResult, String id) {
        this(getResult, id, null);
    }

    public CouchbaseJsonDocument(GetResult getResult, String id, MutationToken mutationToken) {
        super(getResult, id, mutationToken);
        this.content = getResult.contentAsObject().toMap();
    }

    @Override
    public Map<String, Object> getContent() {
        return this.content;
    }

    @Override
    public String toString() {
        return "CouchbaseJsonDocument {" +
                "content=" + content +
                ", id='" + id + '\'' +
                ", expiry=" + expiry +
                ", cas=" + cas +
                ", mutationToken=" + mutationToken +
                '}';
    }
}
