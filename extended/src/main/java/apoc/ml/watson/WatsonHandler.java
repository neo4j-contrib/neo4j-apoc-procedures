package apoc.ml.watson;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_ML_WATSON_URL;
import static apoc.ml.MLUtil.*;
import static apoc.ml.watson.Watson.*;

public interface WatsonHandler {

    enum Type {
        EMBEDDING(new EmbeddingHandler()),
        COMPLETION(new CompletionHandler());

        private final WatsonHandler handler;

        Type(WatsonHandler handler) {
            this.handler = handler;
        }

        public WatsonHandler get() {
            return handler;
        }
    }

    // -- interface methods

    String getDefaultMethod();
    Map<String, Object> getPayload(Map<String, Object> configuration, Object input);

    default String getEndpoint(Map<String, Object> config) {
        var endpoint = config.remove(ENDPOINT_CONF_KEY);
        if (endpoint != null) {
            return (String) endpoint;
        }

        var version = Objects.requireNonNullElse(
                config.remove(API_VERSION_CONF_KEY),
                DEFAULT_VERSION_DATE
        );

        var region = Objects.requireNonNullElse(
                config.remove(REGION_CONF_KEY),
                REGION_CONF_KEY
        );

        String url = "https://%s.ml.cloud.ibm.com/ml/v1/%s?version=%s".formatted(
                region, getDefaultMethod(), version
        );

        return apocConfig().getString(APOC_ML_WATSON_URL, url);
    }


    // -- concrete implementations

    class EmbeddingHandler implements WatsonHandler {
        @Override
        public String getDefaultMethod() {
            return "text/embeddings";
        }

        @Override
        public Map<String, Object> getPayload(Map<String, Object> configuration, Object input) {
            var config = new HashMap<>(configuration);
            config.putIfAbsent(MODEL_ID_KEY, DEFAULT_EMBEDDING_MODEL_ID);
            config.put("inputs", input);
            return config;
        }

    }

    class CompletionHandler implements WatsonHandler {
        @Override
        public String getDefaultMethod() {
            return "text/generation";
        }

        @Override
        public Map<String, Object> getPayload(Map<String, Object> configuration, Object input) {
            var config = new HashMap<>(configuration);
            config.putIfAbsent(MODEL_ID_KEY, DEFAULT_COMPLETION_MODEL_ID);
            config.put("input", input);
            return config;
        }
    }

}
