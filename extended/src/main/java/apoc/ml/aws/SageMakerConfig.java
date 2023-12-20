package apoc.ml.aws;

import java.util.Map;

public class SageMakerConfig extends AWSConfig {
    public static final String ENDPOINT_NAME_KEY = "endpointName";
    
    public SageMakerConfig(Map<String, Object> config) {
        super(config);
    }

    @Override
    String getDefaultEndpoint(Map<String, Object> config) {
        String endpointName = (String) config.get(ENDPOINT_NAME_KEY);
        return endpointName == null
                ? null
                : String.format("https://runtime.sagemaker.%s.amazonaws.com/endpoints/%s/invocations", getRegion(), endpointName);
    }

    @Override
    String getDefaultMethod() {
        return "POST";
    }
}
