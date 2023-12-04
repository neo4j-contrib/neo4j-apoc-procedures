package apoc.ml;


import apoc.ApocConfig;

import java.util.Map;

import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_AZURE_VERSION;
import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_URL;
import static apoc.ml.OpenAI.API_VERSION_CONF_KEY;
import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;

interface OpenAIRequestHandler {

    String getDefaultUrl();
    String getApiVersion(Map<String, Object> configuration, ApocConfig apocConfig);
    void addHeaders(Map<String, Object> headers, String apiKey);

    default String getEndpoint(Map<String, Object> procConfig, ApocConfig apocConfig) {
        return (String) procConfig.getOrDefault(ENDPOINT_CONF_KEY,
                apocConfig.getString(APOC_ML_OPENAI_URL, System.getProperty(APOC_ML_OPENAI_URL, getDefaultUrl())));
    }

    enum Type {
        AZURE(new Azure()), OPENAI(new OpenAi());

        private final OpenAIRequestHandler handler;
        Type(OpenAIRequestHandler handler) {
            this.handler = handler;
        }

        public OpenAIRequestHandler get() {
            return handler;
        }
    }

    class Azure implements OpenAIRequestHandler {
        @Override
        public String getDefaultUrl() {
            return null;
        }

        @Override
        public String getApiVersion(Map<String, Object> configuration, ApocConfig apocConfig) {
            return "?api-version=" + configuration.getOrDefault(API_VERSION_CONF_KEY, apocConfig.getString(APOC_ML_OPENAI_AZURE_VERSION));
        }

        @Override
        public void addHeaders(Map<String, Object> headers, String apiKey) {
            headers.put("api-key", apiKey);
        }
    }

    class OpenAi implements OpenAIRequestHandler {
        @Override
        public String getDefaultUrl() {
            return "https://api.openai.com/v1";
        }

        @Override
        public String getApiVersion(Map<String, Object> configuration, ApocConfig apocConfig) {
            return "";
        }

        @Override
        public void addHeaders(Map<String, Object> headers, String apiKey) {
            headers.put("Authorization", "Bearer " + apiKey);
        }
    }
}
