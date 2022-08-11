package apoc.couchbase.document;

import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;

import java.util.Arrays;

/**
 * Represents a {@link MutationResult} or a {@link GetResult} (in case of apoc.couchbase.get procedure)
 * that contains a <code>JSON object</code> as the
 * content.
 *
 */
public class CouchbaseByteArrayDocument extends CouchbaseObjectDocument<byte[]> {

    /**
     * The byte[] content of the {@link GetResult}.
     */
    public byte[] content;

    public CouchbaseByteArrayDocument(GetResult getResult, String id, MutationToken mutationToken) {
        super(getResult, id, mutationToken);
        this.content = getResult.contentAs(byte[].class);
    }

    @Override
    public byte[] getContent() {
        return this.content;
    }

    @Override
    public String toString() {
        return "CouchbaseByteArrayDocument {" +
                "content=" + Arrays.toString(content) +
                ", id='" + id + '\'' +
                ", expiry=" + expiry +
                ", cas=" + cas +
                ", mutationToken=" + mutationToken +
                '}';
    }
}
