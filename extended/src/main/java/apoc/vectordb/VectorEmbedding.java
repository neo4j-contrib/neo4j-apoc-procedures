package apoc.vectordb;

import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.JSON_PATH_KEY;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorEmbeddingConfig.EMBEDDING_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.METADATA_KEY;

public interface VectorEmbedding {

    enum Type {
        CHROMA(new ChromaEmbeddingType()),
        QDRANT(new QdrantEmbeddingType());

        private final VectorEmbedding embedding;

        Type(VectorEmbedding embedding) {
            this.embedding = embedding;
        }

        public VectorEmbedding get() {
            return embedding;
        }
    }

    public <T> VectorEmbeddingConfig fromGet(Map<String, Object> config,
                                         ProcedureCallContext procedureCallContext,
                                         List<T> ids);

    public VectorEmbeddingConfig fromQuery(Map<String, Object> config,
                                           ProcedureCallContext procedureCallContext,
                                           List<Double> vector,
                                           Map<String, Object> filter,
                                           long limit);
    
    //
    // -- implementations
    //
    
    public static class QdrantEmbeddingType implements VectorEmbedding {

        @Override
        public <T> VectorEmbeddingConfig fromGet(Map<String, Object> config, ProcedureCallContext procedureCallContext, List<T> ids) {
            List<String> fields = procedureCallContext.outputFields().toList();
            config.putIfAbsent(METHOD_KEY, "POST");

            Map<String, Object> additionalBodies = map("ids", ids);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        @Override
        public VectorEmbeddingConfig fromQuery(Map<String, Object> config, ProcedureCallContext procedureCallContext,
                                                      List<Double> vector, Map<String, Object> filter, long limit) {
            List<String> fields = procedureCallContext.outputFields().toList();

            Map<String, Object> additionalBodies = map("vector", vector,
                    "filter", filter,
                    "limit", limit);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        // "with_payload": <boolean> and "with_vectors": <boolean> return the metadata and vector, if true
        // therefore is the RestAPI itself that doesn't return the data if `YIELD ` has not metadata/embedding  
        private static VectorEmbeddingConfig getVectorEmbeddingConfig(Map<String, Object> config, List<String> fields, Map<String, Object> additionalBodies) {
            additionalBodies.put("with_payload", fields.contains("metadata"));
            additionalBodies.put("with_vectors", fields.contains("vector"));

            config.putIfAbsent(EMBEDDING_KEY, "vector");
            config.putIfAbsent(METADATA_KEY, "payload");
            config.putIfAbsent(JSON_PATH_KEY, "result");

            return new VectorEmbeddingConfig(config, Map.of(), additionalBodies);
        }
    }
    
    public static class ChromaEmbeddingType implements VectorEmbedding {

        @Override
        public <T> VectorEmbeddingConfig fromGet(Map<String, Object> config,
                                             ProcedureCallContext procedureCallContext,
                                             List<T> ids) {

            List<String> fields = procedureCallContext.outputFields().toList();

            Map<String, Object> additionalBodies = map("ids", ids);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        @Override
        public VectorEmbeddingConfig fromQuery(Map<String, Object> config,
                                               ProcedureCallContext procedureCallContext,
                                               List<Double> vector,
                                               Map<String, Object> filter,
                                               long limit) {

            List<String> fields = procedureCallContext.outputFields().toList();

            Map<String, Object> additionalBodies = map("query_embeddings", List.of(vector),
                    "where", filter,
                    "n_results", limit);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        // "include": [metadatas, embeddings, ...] return the metadata/embeddings/... if included in the list
        // therefore is the RestAPI itself that doesn't return the data if `YIELD ` has not metadata/embedding  
        private static VectorEmbeddingConfig getVectorEmbeddingConfig(Map<String, Object> config,
                                                                      List<String> fields,
                                                                      Map<String, Object> additionalBodies) {
            ArrayList<String> include = new ArrayList<>();
            if (fields.contains("metadata")) {
                include.add("metadatas");
            }
            if (fields.contains("text")) {
                include.add("documents");
            }
            if (fields.contains("vector")) {
                include.add("embeddings");
            }
            if (fields.contains("score")) {
                include.add("distances");
            }

            additionalBodies.put("include", include);

            return new VectorEmbeddingConfig(config, Map.of(), additionalBodies);
        }
    }
}
