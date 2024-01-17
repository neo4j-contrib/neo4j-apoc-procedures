package apoc.ml.aws;

import java.util.List;
import java.util.Map;

public class ModelItemResult {
    public final String modelId;
    public final String modelArn;
    public final String modelName;
    public final String providerName;
    
    public final Boolean responseStreamingSupported;
    
    public final List<String> customizationsSupported;
    public final List<String> inferenceTypesSupported;
    public final List<String> inputModalities;
    public final List<String> outputModalities;

    public ModelItemResult(Map<String, Object> map) {
        this.modelId = (String) map.get("modelId");
        this.modelArn = (String) map.get("modelArn");
        this.modelName = (String) map.get("modelName");
        this.providerName = (String) map.get("providerName");
        this.responseStreamingSupported = (Boolean) map.get("responseStreamingSupported");
        this.customizationsSupported = (List<String>) map.get("customizationsSupported");
        this.inferenceTypesSupported = (List<String>) map.get("inferenceTypesSupported");
        this.inputModalities = (List<String>) map.get("inputModalities");
        this.outputModalities = (List<String>) map.get("outputModalities");
    }

}
