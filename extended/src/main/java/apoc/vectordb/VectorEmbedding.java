package apoc.vectordb;

import apoc.ml.RestAPIConfig;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.BODY_KEY;
import static apoc.ml.RestAPIConfig.JSON_PATH_KEY;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorEmbeddingConfig.*;

public interface VectorEmbedding {

    enum Type {
        CHROMA(new ChromaEmbeddingType()),
        QDRANT(new QdrantEmbeddingType()),
        WEAVIATE(new WeaviateEmbeddingType());

        private final VectorEmbedding embedding;

        Type(VectorEmbedding embedding) {
            this.embedding = embedding;
        }

        public VectorEmbedding get() {
            return embedding;
        }
    }

    <T> VectorEmbeddingConfig fromGet(Map<String, Object> config,
                                      ProcedureCallContext procedureCallContext,
                                      List<T> ids);

    VectorEmbeddingConfig fromQuery(Map<String, Object> config,
                                    ProcedureCallContext procedureCallContext,
                                    List<Double> vector,
                                    Object filter,
                                    long limit,
                                    String collection);
    
    //
    // -- implementations
    //
    
    class QdrantEmbeddingType implements VectorEmbedding {

        @Override
        public <T> VectorEmbeddingConfig fromGet(Map<String, Object> config, ProcedureCallContext procedureCallContext, List<T> ids) {
            List<String> fields = procedureCallContext.outputFields().toList();
            config.putIfAbsent(METHOD_KEY, "POST");

            Map<String, Object> additionalBodies = map("ids", ids);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        @Override
        public VectorEmbeddingConfig fromQuery(Map<String, Object> config, ProcedureCallContext procedureCallContext,
                                                      List<Double> vector, Object filter, long limit,
                                               String collection) {
            List<String> fields = procedureCallContext.outputFields().toList();

            Map<String, Object> additionalBodies = map("vector", vector,
                    "filter", filter,
                    "limit", limit);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        // "with_payload": <boolean> and "with_vectors": <boolean> return the metadata and vector, if true
        // therefore is the RestAPI itself that doesn't return the data if `YIELD ` has not metadata/embedding  
        private static VectorEmbeddingConfig getVectorEmbeddingConfig(Map<String, Object> config, List<String> fields, Map<String, Object> additionalBodies) {
            config.putIfAbsent(VECTOR_KEY, "vector");
            config.putIfAbsent(METADATA_KEY, "payload");
            config.putIfAbsent(JSON_PATH_KEY, "result");
            
            VectorEmbeddingConfig conf = new VectorEmbeddingConfig(config);
            additionalBodies.put("with_payload", fields.contains("metadata"));
            additionalBodies.put("with_vectors", fields.contains("vector") && conf.isAllResults());

            return populateApiBodyRequest(conf, additionalBodies);
        }
    }
    
    class ChromaEmbeddingType implements VectorEmbedding {

        @Override
        public <T> VectorEmbeddingConfig fromGet(Map<String, Object> config,
                                             ProcedureCallContext procedureCallContext,
                                             List<T> ids) {

            List<String> fields = procedureCallContext.outputFields().toList();

            VectorEmbeddingConfig conf = new VectorEmbeddingConfig(config);
            Map<String, Object> additionalBodies = map("ids", ids);

            return getVectorEmbeddingConfig(conf, fields, additionalBodies);
        }

        @Override
        public VectorEmbeddingConfig fromQuery(Map<String, Object> config,
                                               ProcedureCallContext procedureCallContext,
                                               List<Double> vector,
                                               Object filter,
                                               long limit,
                                               String collection) {

            List<String> fields = procedureCallContext.outputFields().toList();

            VectorEmbeddingConfig conf = new VectorEmbeddingConfig(config);
            Map<String, Object> additionalBodies = map("query_embeddings", List.of(vector),
                    "where", filter,
                    "n_results", limit);

            return getVectorEmbeddingConfig(conf, fields, additionalBodies);
        }

        // "include": [metadatas, embeddings, ...] return the metadata/embeddings/... if included in the list
        // therefore is the RestAPI itself that doesn't return the data if `YIELD ` has not metadata/embedding  
        private static VectorEmbeddingConfig getVectorEmbeddingConfig(VectorEmbeddingConfig config,
                                                                      List<String> fields,
                                                                      Map<String, Object> additionalBodies) {
            ArrayList<String> include = new ArrayList<>();
            if (fields.contains("metadata")) {
                include.add("metadatas");
            }
            if (fields.contains("text") && config.isAllResults()) {
                include.add("documents");
            }
            if (fields.contains("vector") && config.isAllResults()) {
                include.add("embeddings");
            }
            if (fields.contains("score")) {
                include.add("distances");
            }

            additionalBodies.put("include", include);

            return populateApiBodyRequest(config, additionalBodies);
        }
    }

    class WeaviateEmbeddingType implements VectorEmbedding {

        @Override
        public <T> VectorEmbeddingConfig fromGet(Map<String, Object> config, ProcedureCallContext procedureCallContext, List<T> ids) {
            config.putIfAbsent(BODY_KEY, null);
            return populateApiBodyRequest(getVectorEmbeddingConfig(config), Map.of());
        }
        
        @Override
        public VectorEmbeddingConfig fromQuery(Map<String, Object> config, ProcedureCallContext procedureCallContext, List<Double> vector, Object filter, long limit, String collection) {
            List<String> fields = procedureCallContext.outputFields().toList();
            config.putIfAbsent(METHOD_KEY, "POST");
            VectorEmbeddingConfig vectorEmbeddingConfig = getVectorEmbeddingConfig(config);
            
            List list = (List) config.get("fields");
            if (list == null) {
                throw new RuntimeException("You have to define `field` list of parameter to be returned");
            }
            Object fieldList = String.join("\n", list);

            filter = filter == null 
                    ? "" 
                    : ", where: " + filter;

            String includeVector = (fields.contains("vector") && vectorEmbeddingConfig.isAllResults()) ? ",vector" : "";
            String additional = "_additional {id, distance " + includeVector  + "}";
            String query = """
                  {
                      Get {
                        %s(limit: %s, nearVector: {vector: %s } %s) {%s  %s}
                      }
                  }
                    """.formatted(
                          collection, limit, vector, filter, fieldList, additional
            );

            Map<String, Object> additionalBodies = map("query", query);
            
            return populateApiBodyRequest(vectorEmbeddingConfig, additionalBodies);
        }

        private static VectorEmbeddingConfig getVectorEmbeddingConfig(Map<String, Object> config) {
            config.putIfAbsent(VECTOR_KEY, "vector");
            config.putIfAbsent(METADATA_KEY, "properties");
            
            return new VectorEmbeddingConfig(config);
        }
    }

    
    static VectorEmbeddingConfig populateApiBodyRequest(VectorEmbeddingConfig config,
                                                        Map<String, Object> additionalBodies) {

        RestAPIConfig apiConfig = config.getApiConfig();
        Map<String, Object> body = apiConfig.getBody();
        if (body != null) additionalBodies.forEach(body::putIfAbsent);
        apiConfig.setBody(body);
        return config;
    }
}
