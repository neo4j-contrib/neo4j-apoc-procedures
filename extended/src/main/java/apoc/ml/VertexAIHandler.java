package apoc.ml;


import apoc.ApocConfig;

import java.util.Collection;
import java.util.Map;

import static apoc.ExtendedApocConfig.APOC_ML_VERTEXAI_URL;
import static apoc.ml.VertexAI.getParameters;
import static org.apache.commons.lang3.StringUtils.isBlank;

public abstract class VertexAIHandler {
    public static final String ENDPOINT_CONF_KEY = "endpoint";
    public static final String MODEL_CONF_KEY = "model";
    public static final String RESOURCE_CONF_KEY = "resource";
    
    public static final String STREAM_RESOURCE = "streamGenerateContent";
    public static final String PREDICT_RESOURCE = "predict";
    
    private static final String DEFAULT_BASE_URL = "https://%1$s-aiplatform.googleapis.com/v1/projects/%2$s/locations/%1$s/publishers/google/models/%3$s:%4$s";
    public static final String DEFAULT_REGION = "us-central1";
    
    public abstract String getDefaultResource();
    
    public abstract Map<String, Object> getBody(Object inputs, Map<String, Object> configuration, Collection<String> retainKeys);
    
    public abstract String getJsonPath();
    
    public String getFullUrl(Map<String, Object> configuration, ApocConfig apocConfig, String defaultModel, String project) {
        String model = configuration.getOrDefault(MODEL_CONF_KEY, defaultModel).toString();
        String region = configuration.getOrDefault("region", DEFAULT_REGION).toString();
        String resource = configuration.getOrDefault(RESOURCE_CONF_KEY, getDefaultResource()).toString();
        
        String endpoint = getUrlTemplate(configuration, apocConfig);
        
        if (isBlank(endpoint) && isBlank(project)) {
                throw new IllegalArgumentException("Either project parameter or endpoint config. must not be empty");
        }
        return String.format(endpoint, region, project, model, resource);
    }

    private String getUrlTemplate(Map<String, Object> procConfig, ApocConfig apocConfig) {
        return (String) procConfig.getOrDefault(ENDPOINT_CONF_KEY,
                apocConfig.getString(APOC_ML_VERTEXAI_URL, System.getProperty(APOC_ML_VERTEXAI_URL, DEFAULT_BASE_URL)));
    }

    enum Type {
        PREDICT(new Predict()),
        STREAM(new Stream()),
        CUSTOM(new Custom());
        
        private final VertexAIHandler handler;
        Type(VertexAIHandler handler) {
            this.handler = handler;
        }

        public VertexAIHandler get() {
            return handler;
        }
    }

    private static class Predict extends VertexAIHandler {

        @Override
        public String getDefaultResource() {
            return PREDICT_RESOURCE;
        }

        @Override
        public Map<String, Object> getBody(Object inputs, Map<String, Object> configuration, Collection<String> retainKeys) {
            return Map.of("instances", inputs, "parameters", getParameters(configuration, retainKeys));
        }

        @Override
        public String getJsonPath() {
            return "$.predictions";
        }
    }

    private static class Stream extends VertexAIHandler {

        @Override
        public String getDefaultResource() {
            return STREAM_RESOURCE;
        }

        @Override
        public Map<String, Object> getBody(Object inputs, Map<String, Object> configuration, Collection<String> retainKeys) {
            return Map.of("contents", inputs, "generation_config", getParameters(configuration, retainKeys));
        }

        @Override
        public String getJsonPath() {
            return "$[0].candidates";
        }
    }

    private static class Custom extends VertexAIHandler {

        @Override
        public String getDefaultResource() {
            return STREAM_RESOURCE;
        }

        @Override
        public Map<String, Object> getBody(Object inputs, Map<String, Object> configuration, Collection<String> retainKeys) {
            return (Map<String, Object>) inputs;
        }

        @Override
        public String getJsonPath() {
            return null;
        }
    }
}
    
    