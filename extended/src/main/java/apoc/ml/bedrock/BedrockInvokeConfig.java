package apoc.ml.bedrock;

import java.util.Map;

public class BedrockInvokeConfig extends BedrockConfig {
    public static final String MODEL_ID = "modelId";

    public BedrockInvokeConfig(Map<String, Object> config) {
        super(config);
    }

    @Override
    String getDefaultEndpoint(Map<String, Object> config) {
        String modelId = (String) config.get(MODEL_ID);
        return modelId == null
                ? null
                : String.format("https://bedrock-runtime.us-east-1.amazonaws.com/model/%s/invoke", modelId);
    }

    @Override
    String getDefaultMethod() {
        return "POST";
    }
}