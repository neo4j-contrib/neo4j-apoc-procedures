package apoc.systemdb;

import apoc.systemdb.metadata.ExportMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SystemDbConfig {
    public static final String FEATURES_KEY = "features";
    public static final String FILENAME_KEY = "fileName";

    private final List<String> features;
    private final String fileName;

    public SystemDbConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        List<String> DEFAULT_FEATURES = Stream.of(ExportMetadata.Type.values())
                .map(Enum::name)
                .collect(Collectors.toList());
        this.features = (List<String>) config.getOrDefault(FEATURES_KEY, DEFAULT_FEATURES);
        this.fileName = (String) config.getOrDefault(FILENAME_KEY, "metadata");
    }

    public List<String> getFeatures() {
        return features;
    }

    public String getFileName() {
        return fileName;
    }
}