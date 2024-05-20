package apoc.vectordb;

import apoc.ml.RestAPIConfig;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ml.RestAPIConfig.BODY_KEY;
import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.ml.RestAPIConfig.JSON_PATH_KEY;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorEmbeddingConfig.*;

public interface VectorEmbeddingHandler {

    <T> VectorEmbeddingConfig fromGet(Map<String, Object> config,
                                      ProcedureCallContext procedureCallContext,
                                      List<T> ids,
                                      String collection);

    VectorEmbeddingConfig fromQuery(Map<String, Object> config,
                                    ProcedureCallContext procedureCallContext,
                                    List<Double> vector,
                                    Object filter,
                                    long limit,
                                    String collection);

    default VectorEmbeddingConfig populateApiBodyRequest(VectorEmbeddingConfig config,
                                                        Map<String, Object> additionalBodies) {

        RestAPIConfig apiConfig = config.getApiConfig();
        Map<String, Object> body = apiConfig.getBody();
        if (body != null) additionalBodies.forEach(body::putIfAbsent);
        apiConfig.setBody(body);
        return config;
    }
}
