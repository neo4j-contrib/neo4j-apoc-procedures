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
     */
//    public record EmbeddingResult(
//            Object id, Double score, List<Double> vector, Map<String, Object> metadata, String text, Entity entity) {}


    public static class EmbeddingResult {

        public final Object id;
        public final Double score;
        public final List<Double> vector;
        public final Map<String, Object> metadata;
        public final String text;
        // we cannot declare entity with class Entity, 
        // as an error  `cannot be converted to a Neo4j type: Don't know how to map `org.neo4j.graphdb.Entity` to the Neo4j Type` would be thrown
        public final Object entity;

        public EmbeddingResult(Object id, Double score, List<Double> vector, Map<String, Object> metadata, String text, Object entity) {
            this.id = id;
            this.score = score;
            this.vector = vector;
            this.metadata = metadata;
            this.text = text;
            this.entity = entity;
        }
    }
    
//    public static class NodeEmbeddingResult extends EmbeddingResult<Node> {
//        public NodeEmbeddingResult(Object id, Double score, List<Double> vector, Map<String, Object> metadata, String text, Node entity) {
//            super(id, score, vector, metadata, text, entity);
//        }
//    }
//
//    public static class RelEmbeddingResult extends EmbeddingResult<Relationship> {
//
//        public RelEmbeddingResult(Object id, Double score, List<Double> vector, Map<String, Object> metadata, String text, Relationship entity) {
//            super(id, score, vector, metadata, text, entity);
//        }
//    }

}
