package apoc.vectordb;


import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;

public class VectorDbUtil {
    
    /**
     * we can configure the endpoint via config map or via hostOrKey parameter,
     * to handle potential endpoint changes.
     * For example, in Qdrant `BASE_URL/collections/COLLECTION_NAME/points` could change in the future.
     */
    public static void getEndpoint(Map<String, Object> config, String endpoint) {
        config.putIfAbsent(ENDPOINT_KEY, endpoint);
    }

    /**
     * Result of `apoc.vectordb.*.get` and `apoc.vectordb.*.query` procedures
     *
     * @param entity we cannot declare entity with class Entity, 
     *               as an error `cannot be converted to a Neo4j type: Don't know how to map `org.neo4j.graphdb.Entity` to the Neo4j Type` would be thrown
     */
    public record EmbeddingResult(
            Object id, Double score, List<Double> vector, Map<String, Object> metadata, String text, Object entity) {}

}
