package apoc.ml.aws;

import java.util.Map;
import java.util.regex.Pattern;

public class SageMakerConfig extends AWSConfig {
    public static final String ENDPOINT_NAME_KEY = "endpointName";
    public static final Pattern SERVICE_HOST_PATTERN = Pattern.compile("([a-z0-9-]+\\.)?sagemaker\\.[a-z0-9-]+\\.amazonaws\\.com");
    
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
    Pattern getServiceHostPattern() {
        return SERVICE_HOST_PATTERN;
    }

    @Override
    String getDefaultMethod() {
        return "POST";
    }
}
