package apoc.vectordb;

import static apoc.vectordb.VectorEmbeddingConfig.*;

import apoc.ml.RestAPIConfig;
import java.util.List;
import java.util.Map;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

public interface VectorEmbeddingHandler {

    <T> VectorEmbeddingConfig fromGet(
            Map<String, Object> config, ProcedureCallContext procedureCallContext, List<T> ids, String collection);

    VectorEmbeddingConfig fromQuery(
            Map<String, Object> config,
            ProcedureCallContext procedureCallContext,
            List<Double> vector,
            Object filter,
            long limit,
            String collection);

    default VectorEmbeddingConfig populateApiBodyRequest(
            VectorEmbeddingConfig config, Map<String, Object> additionalBodies) {

        RestAPIConfig apiConfig = config.getApiConfig();
        Map<String, Object> body = apiConfig.getBody();
        if (body != null) additionalBodies.forEach(body::putIfAbsent);
        apiConfig.setBody(body);
        return config;
    }
}
