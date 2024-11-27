package apoc.vectordb;

import static apoc.ml.RestAPIConfig.BODY_KEY;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorEmbeddingConfig.METADATA_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.VECTOR_KEY;

import apoc.util.UrlResolver;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

public class WeaviateHandler implements VectorDbHandler {

    @Override
    public String getUrl(String hostOrKey) {
        String url = new UrlResolver("http", "localhost", 8000).getUrl("weaviate", hostOrKey);
        return url + "/v1";
    }

    @Override
    public VectorEmbeddingHandler getEmbedding() {
        return new WeaviateEmbeddingHandler();
    }

    @Override
    public String getLabel() {
        return "Weaviate";
    }

    // -- embedding handler
    static class WeaviateEmbeddingHandler implements VectorEmbeddingHandler {

        @Override
        public <T> VectorEmbeddingConfig fromGet(
                Map<String, Object> config, ProcedureCallContext procedureCallContext, List<T> ids) {
            config.putIfAbsent(BODY_KEY, null);
            return VectorEmbeddingHandler.populateApiBodyRequest(getVectorEmbeddingConfig(config), Map.of());
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
            config.putIfAbsent(METHOD_KEY, "POST");
            VectorEmbeddingConfig vectorEmbeddingConfig = getVectorEmbeddingConfig(config);

            List list = (List) config.get("fields");
            if (list == null) {
                throw new RuntimeException("You have to define `field` list of parameter to be returned");
            }
            Object fieldList = String.join("\n", list);

            filter = filter == null ? "" : ", where: " + filter;

            String includeVector = (fields.contains("vector") && vectorEmbeddingConfig.isAllResults()) ? ",vector" : "";
            String additional = "_additional {id, distance " + includeVector + "}";
            String query = String.format(
                    "{\n" + "    Get {\n"
                            + "      %s(limit: %s, nearVector: {vector: %s } %s) {%s  %s}\n"
                            + "    }\n"
                            + "}",
                    collection, limit, vector, filter, fieldList, additional);

            Map<String, Object> additionalBodies = map("query", query);

            return VectorEmbeddingHandler.populateApiBodyRequest(vectorEmbeddingConfig, additionalBodies);
        }

        private static VectorEmbeddingConfig getVectorEmbeddingConfig(Map<String, Object> config) {
            config.putIfAbsent(VECTOR_KEY, "vector");
            config.putIfAbsent(METADATA_KEY, "properties");

            return new VectorEmbeddingConfig(config);
        }
    }
}
