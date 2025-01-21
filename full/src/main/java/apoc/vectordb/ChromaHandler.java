package apoc.vectordb;

import static apoc.util.MapUtil.map;

import apoc.util.UrlResolver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

public class ChromaHandler implements VectorDbHandler {
    @Override
    public String getUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 8000).getUrl("chroma", hostOrKey);
    }

    @Override
    public VectorEmbeddingHandler getEmbedding() {
        return new ChromaEmbeddingHandler();
    }

    @Override
    public String getLabel() {
        return "Chroma";
    }

    // -- embedding handler
    static class ChromaEmbeddingHandler implements VectorEmbeddingHandler {

        @Override
        public <T> VectorEmbeddingConfig fromGet(
                Map<String, Object> config, ProcedureCallContext procedureCallContext, List<T> ids, String collection) {

            List<String> fields = procedureCallContext.outputFields().collect(Collectors.toList());

            VectorEmbeddingConfig conf = new VectorEmbeddingConfig(config);
            Map<String, Object> additionalBodies = map("ids", ids);

            return getVectorEmbeddingConfig(conf, fields, additionalBodies);
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

            VectorEmbeddingConfig conf = new VectorEmbeddingConfig(config);
            Map<String, Object> additionalBodies =
                    map("query_embeddings", List.of(vector), "where", filter, "n_results", limit);

            return getVectorEmbeddingConfig(conf, fields, additionalBodies);
        }

        // "include": [metadatas, embeddings, ...] return the metadata/embeddings/... if included in the list
        // therefore is the RestAPI itself that doesn't return the data if `YIELD ` has not metadata/embedding
        private VectorEmbeddingConfig getVectorEmbeddingConfig(
                VectorEmbeddingConfig config, List<String> fields, Map<String, Object> additionalBodies) {
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
}
