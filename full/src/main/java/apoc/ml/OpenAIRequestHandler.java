package apoc.ml;

import static apoc.ApocConfig.APOC_ML_OPENAI_AZURE_VERSION;
import static apoc.ApocConfig.APOC_ML_OPENAI_URL;
import static apoc.ml.MLUtil.API_VERSION_CONF_KEY;
import static apoc.ml.MLUtil.ENDPOINT_CONF_KEY;
import static apoc.ml.MixedbreadAI.ERROR_MSG_MISSING_ENDPOINT;
import static apoc.ml.MixedbreadAI.MIXEDBREAD_BASE_URL;

import apoc.ApocConfig;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

abstract class OpenAIRequestHandler {

    private final String defaultUrl;

    public OpenAIRequestHandler(String defaultUrl) {
        this.defaultUrl = defaultUrl;
    }

    public String getDefaultUrl() {
        return defaultUrl;
    }

    public abstract String getApiVersion(Map<String, Object> configuration, ApocConfig apocConfig);

    public abstract void addApiKey(Map<String, Object> headers, String apiKey);

    public String getEndpoint(Map<String, Object> procConfig, ApocConfig apocConfig) {
        return (String) procConfig.getOrDefault(
                ENDPOINT_CONF_KEY,
                apocConfig.getString(APOC_ML_OPENAI_URL, System.getProperty(APOC_ML_OPENAI_URL, getDefaultUrl())));
    }

    public String getFullUrl(String method, Map<String, Object> procConfig, ApocConfig apocConfig) {
        return Stream.of(getEndpoint(procConfig, apocConfig), method, getApiVersion(procConfig, apocConfig))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.joining("/"));
    }

    enum Type {
        AZURE(new Azure(null)),
        MIXEDBREAD_EMBEDDING(new OpenAi(MIXEDBREAD_BASE_URL)),
        MIXEDBREAD_CUSTOM(new Custom()),
        OPENAI(new OpenAi("https://api.openai.com/v1"));

        private final OpenAIRequestHandler handler;

        Type(OpenAIRequestHandler handler) {
            this.handler = handler;
        }

        public OpenAIRequestHandler get() {
            return handler;
        }
    }

    static class Azure extends OpenAIRequestHandler {

        public Azure(String defaultUrl) {
            super(defaultUrl);
        }

        @Override
        public String getApiVersion(Map<String, Object> configuration, ApocConfig apocConfig) {
            return "?api-version="
                    + configuration.getOrDefault(
                            API_VERSION_CONF_KEY, apocConfig.getString(APOC_ML_OPENAI_AZURE_VERSION));
        }

        @Override
        public void addApiKey(Map<String, Object> headers, String apiKey) {
            headers.put("api-key", apiKey);
        }
    }

    static class OpenAi extends OpenAIRequestHandler {

        public OpenAi(String defaultUrl) {
            super(defaultUrl);
        }

        @Override
        public String getApiVersion(Map<String, Object> configuration, ApocConfig apocConfig) {
            return "";
        }

        @Override
        public void addApiKey(Map<String, Object> headers, String apiKey) {
            headers.put("Authorization", "Bearer " + apiKey);
        }
    }

    static class Custom extends OpenAi {
        public Custom() {
            super(null);
        }

        @Override
        public String getDefaultUrl() {
            throw new RuntimeException(ERROR_MSG_MISSING_ENDPOINT);
        }
    }
}
