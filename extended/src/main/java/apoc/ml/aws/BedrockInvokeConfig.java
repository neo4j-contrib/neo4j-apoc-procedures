package apoc.ml.aws;

import apoc.util.Util;

import java.util.Map;
import java.util.regex.Pattern;

public class BedrockInvokeConfig extends AWSConfig {
    public static final String MODEL = "model";
    public static final String OPEN_AI_COMPATIBLE = "openAICompatible";

    private final boolean openAICompatible;

    public BedrockInvokeConfig(Map<String, Object> config) {
        super(config);
        this.openAICompatible = Util.toBoolean(config.get(OPEN_AI_COMPATIBLE));
    }

    @Override
    String getDefaultEndpoint(Map<String, Object> config) {
        String modelId = (String) config.get(MODEL);
        return modelId == null
                ? null
                : String.format("https://bedrock-runtime.%s.amazonaws.com/model/%s/invoke", getRegion(), modelId);
    }

    @Override
    Pattern getServiceHostPattern() {
        return BedrockUtil.SERVICE_HOST_PATTERN;
    }

    @Override
    String getDefaultMethod() {
        return "POST";
    }

    public boolean isOpenAICompatible() {
        return openAICompatible;
    }
}