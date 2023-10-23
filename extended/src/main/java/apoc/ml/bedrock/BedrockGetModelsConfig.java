package apoc.ml.bedrock;

import java.util.Map;

public class BedrockGetModelsConfig extends BedrockConfig {
    public static final String DEFAULT_PATH = "foundation-models";
    public static final String PATH_GET = "path";

    public BedrockGetModelsConfig(Map<String, Object> config) {
        super(config);
    }

    @Override
    String getDefaultEndpoint(Map<String, Object> config) {
        String path = (String) config.getOrDefault(PATH_GET, DEFAULT_PATH);
        return "https://bedrock.us-east-1.amazonaws.com/" + path;
    }

    @Override
    String getDefaultMethod() {
        return "GET";
    }
}
