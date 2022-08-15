package apoc.couchbase.document;

import java.util.Map;

import org.neo4j.procedure.Procedure;

import com.couchbase.client.java.AsyncBucket;

/**
 * This interface defines a Couchbase Document interface
 * with valid data types for neo4j procedures.
 * <p/>
 * it represents a Couchbase Server document which is stored in and
 * retrieved from a {@link AsyncBucket}.
 * 
 * @since 15.8.2016
 * @author inserpio
 * @see Procedure
 */
public interface CouchbaseDocument<T> {

	String getId();

	T getContent();

	long getCas();

	long getExpiry();

	Map<String, Object> getMutationToken();
}
