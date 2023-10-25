package apoc.ml.bedrock;

import java.util.Map;

public class BedrockInvokeConfig extends BedrockConfig {
    public static final String MODEL = "model";

    public BedrockInvokeConfig(Map<String, Object> config) {
        super(config);
    }

    @Override
    String getDefaultEndpoint(Map<String, Object> config) {
        String modelId = (String) config.get(MODEL);
        return modelId == null
                ? null
                : String.format("https://bedrock-runtime.%s.amazonaws.com/model/%s/invoke", getRegion(), modelId);
    }

    @Override
    String getDefaultMethod() {
        return "POST";
    }
}