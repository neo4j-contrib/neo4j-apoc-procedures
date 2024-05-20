package apoc.vectordb;

import apoc.ml.RestAPIConfig;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ml.RestAPIConfig.BODY_KEY;
import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorEmbeddingConfig.VECTOR_KEY;

public class PineconeHandler implements VectorDbHandler {

    @Override
    public String getUrl(String hostOrKey) {
        return StringUtils.isBlank(hostOrKey)
                ? "https://api.pinecone.io"
                : hostOrKey;
    }

    @Override
    public VectorEmbeddingHandler getEmbedding() {
        return new PineconeEmbeddingHandler();
    }

    @Override
    public String getLabel() {
        return "Pinecone";
    }

    @Override
    public Map<String, Object> getCredentials(Object credentialsObj, Map<String, Object> config) {
        Map headers = (Map) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        headers.putIfAbsent("Api-Key", credentialsObj);
        config.put(HEADERS_KEY, headers);
        return config;
    }

    // -- embedding handler
    static class PineconeEmbeddingHandler implements VectorEmbeddingHandler {

        /**
         * "method" should be "GET", but is null as a workaround.
         *  Since with `method: POST` the {@link apoc.util.Util#openUrlConnection(URL, Map)} has a `setChunkedStreamingMode`
         *  that makes the request to respond 200 OK, but returns an empty result 
         */
        @Override
        public <T> VectorEmbeddingConfig fromGet(Map<String, Object> config, ProcedureCallContext procedureCallContext, List<T> ids, String collection) {
            List<String> fields = procedureCallContext.outputFields().toList();
            
            config.put(BODY_KEY, null);
            Map<String, Object> headers = (Map<String, Object>) config.getOrDefault(HEADERS_KEY, new HashMap<>());
            headers.remove(METHOD_KEY);
            headers.remove("content-type");
            config.put(HEADERS_KEY, headers);

            String endpoint = (String) config.get(ENDPOINT_KEY);
            if (!endpoint.contains("ids=")) {
                String idsQueryUrl = ids.stream().map(i -> "ids=" + i).collect(Collectors.joining("&"));

                if (endpoint.contains("?")) {
                    endpoint += "&" + idsQueryUrl;
                } else {
                    endpoint += "?" + idsQueryUrl;
                }
            }

            config.put(ENDPOINT_KEY, endpoint);
            return getVectorEmbeddingConfig(config, fields, map());
        }

        @Override
        public VectorEmbeddingConfig fromQuery(Map<String, Object> config, ProcedureCallContext procedureCallContext, List<Double> vector, Object filter, long limit, String collection) {
            List<String> fields = procedureCallContext.outputFields().toList();

            config.putIfAbsent(METHOD_KEY, "POST");
            Map<String, Object> additionalBodies = map("vector", vector,
                    "filter", filter,
                    "topK", limit);

            return getVectorEmbeddingConfig(config, fields, additionalBodies);
        }

        private VectorEmbeddingConfig getVectorEmbeddingConfig(Map<String, Object> config, List<String> fields, Map<String, Object> additionalBodies) {
            config.putIfAbsent(VECTOR_KEY, "values");

            VectorEmbeddingConfig conf = new VectorEmbeddingConfig(config);

            additionalBodies.put("includeMetadata", fields.contains("metadata"));
            additionalBodies.put("includeValues", fields.contains("vector") && conf.isAllResults());

            RestAPIConfig apiConfig = conf.getApiConfig();
            Map<String, Object> headers = apiConfig.getHeaders();
            headers.remove(METHOD_KEY);
            apiConfig.setHeaders(headers);
            
            return populateApiBodyRequest(conf, additionalBodies);
        }
    }
}