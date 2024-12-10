package apoc.vectordb;

import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorDbUtil.addMetadataKeyToFields;
import static apoc.vectordb.VectorEmbeddingConfig.META_AS_SUBKEY_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.SCORE_KEY;

import apoc.util.UrlResolver;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

public class MilvusHandler implements VectorDbHandler {

    @Override
    public String getUrl(String hostOrKey) {
        String url = new UrlResolver("http", "localhost", 19530).getUrl("milvus", hostOrKey);
        return url + "/v2/vectordb";
    }

    @Override
    public VectorEmbeddingHandler getEmbedding() {
        return new MilvusEmbeddingHandler();
    }

    @Override
    public String getLabel() {
        return "Milvus";
    }

    // -- embedding handler
    static class MilvusEmbeddingHandler implements VectorEmbeddingHandler {

        @Override
        public <T> VectorEmbeddingConfig fromGet(
                Map<String, Object> config, ProcedureCallContext procedureCallContext, List<T> ids, String collection) {
            List<String> fields = procedureCallContext.outputFields().collect(Collectors.toList());
            Map<String, Object> additionalBodies = map("id", ids);

            return getVectorEmbeddingConfig(config, fields, collection, additionalBodies);
        }

        @Override
        public VectorEmbeddingConfig fromQuery(
                Map<String, Object> config,
                ProcedureCallContext procedureCallContext,
                List<Double> vector,
                Object filter,
                long limit,
                String collection) {
            config.putIfAbsent(SCORE_KEY, "distance");

            List<String> fields = procedureCallContext.outputFields().collect(Collectors.toList());
            Map<String, Object> additionalBodies = map("data", List.of(vector), "limit", limit);
            if (filter != null) {
                additionalBodies.put("filter", filter);
            }

            return getVectorEmbeddingConfig(config, fields, collection, additionalBodies);
        }

        private VectorEmbeddingConfig getVectorEmbeddingConfig(
                Map<String, Object> config,
                List<String> procFields,
                String collection,
                Map<String, Object> additionalBodies) {
            config.putIfAbsent(META_AS_SUBKEY_KEY, false);

            List listFields = addMetadataKeyToFields(config);

            if (procFields.contains("vector") && !listFields.contains("vector")) {
                listFields.add("vector");
            }
            VectorEmbeddingConfig conf = new VectorEmbeddingConfig(config);
            additionalBodies.put("collectionName", collection);
            additionalBodies.put("outputFields", listFields);

            return VectorEmbeddingHandler.populateApiBodyRequest(conf, additionalBodies);
        }
    }
}
