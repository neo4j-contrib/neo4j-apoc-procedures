package apoc.vectordb;

import apoc.ml.RestAPIConfig;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.List;
import java.util.Map;

import static apoc.vectordb.VectorEmbeddingConfig.*;

public interface VectorEmbeddingHandler {

    <T> VectorEmbeddingConfig fromGet(Map<String, Object> config,
                                      ProcedureCallContext procedureCallContext,
                                      List<T> ids);

    VectorEmbeddingConfig fromQuery(Map<String, Object> config,
                                    ProcedureCallContext procedureCallContext,
                                    List<Double> vector,
                                    Object filter,
                                    long limit,
                                    String collection);

   

    static VectorEmbeddingConfig populateApiBodyRequest(VectorEmbeddingConfig config,
                                                        Map<String, Object> additionalBodies) {

        RestAPIConfig apiConfig = config.getApiConfig();
        Map<String, Object> body = apiConfig.getBody();
        if (body != null) additionalBodies.forEach(body::putIfAbsent);
        apiConfig.setBody(body);
        return config;
    }
}
