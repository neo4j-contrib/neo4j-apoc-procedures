package apoc.vectordb;

import static apoc.ml.RestAPIConfig.JSON_PATH_KEY;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorEmbeddingConfig.METADATA_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.VECTOR_KEY;

import apoc.util.UrlResolver;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

public class QdrantHandler implements VectorDbHandler {

    @Override
    public String getUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 6333).getUrl("qdrant", hostOrKey);
    }

    @Override
    public VectorEmbeddingHandler getEmbedding() {
        return new QdrantEmbeddingHandler();
    }

    @Override
    public String getLabel() {
        return "Qdrant";
    }

    // -- embedding handler
    static class QdrantEmbeddingHandler implements VectorEmbeddingHandler {

        @Override
        public <T> VectorEmbeddingConfig fromGet(
                Map<String, Object> config, ProcedureCallContext procedureCallContext, List<T> ids) {
            List<String> fields = procedureCallContext.outputFields().collect(Collectors.toList());
            config.putIfAbsent(METHOD_KEY, "POST");

            Map<String, Object> additionalBodies = map("ids", ids);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        @Override
        public VectorEmbeddingConfig fromQuery(
                Map<String, Object> config,
                ProcedureCallContext procedureCallContext,
                List<Double> vector,
                Object filter,
                long limit,
                String collection) {
            List<String> fields = procedureCallContext.outputFields().collect(Collectors.toList());

            Map<String, Object> additionalBodies = map("vector", vector, "filter", filter, "limit", limit);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        // "with_payload": <boolean> and "with_vectors": <boolean> return the metadata and vector, if true
        // therefore is the RestAPI itself that doesn't return the data if `YIELD ` has not metadata/embedding
        private static VectorEmbeddingConfig getVectorEmbeddingConfig(
                Map<String, Object> config, List<String> fields, Map<String, Object> additionalBodies) {
            config.putIfAbsent(VECTOR_KEY, "vector");
            config.putIfAbsent(METADATA_KEY, "payload");
            config.putIfAbsent(JSON_PATH_KEY, "result");

            VectorEmbeddingConfig conf = new VectorEmbeddingConfig(config);
            additionalBodies.put("with_payload", fields.contains("metadata"));
            additionalBodies.put("with_vectors", fields.contains("vector") && conf.isAllResults());

            return VectorEmbeddingHandler.populateApiBodyRequest(conf, additionalBodies);
        }
    }
}
