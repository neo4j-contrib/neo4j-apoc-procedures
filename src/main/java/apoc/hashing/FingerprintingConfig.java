package apoc.hashing;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FingerprintingConfig {
    private final String digestAlgorithm;
    private final Map<String, List<String>> nodeWhiteList;
    private final Map<String, List<String>> relWhiteList;

    private final Map<String, List<String>> nodeBlackList;
    private final Map<String, List<String>> relBlackList;

    private final List<String> mapWhiteList;
    private final List<String> mapBlackList;


    public FingerprintingConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.digestAlgorithm = (String) config.getOrDefault("digestAlgorithm", "MD5");

        this.nodeWhiteList = (Map<String, List<String>>) config.getOrDefault("nodeWhiteList", Collections.emptyMap());
        this.relWhiteList = (Map<String, List<String>>) config.getOrDefault("relWhiteList", Collections.emptyMap());
        this.nodeBlackList = (Map<String, List<String>>) config.getOrDefault("nodeBlackList", Collections.emptyMap());
        this.relBlackList = (Map<String, List<String>>) config.getOrDefault("relBlackList", Collections.emptyMap());
        this.mapWhiteList = (List<String>) config.getOrDefault("mapWhiteList", Collections.emptyList());
        this.mapBlackList = (List<String>) config.getOrDefault("mapBlackList", config.getOrDefault("propertyExcludes", Collections.emptyList()));

        validateConfig();
    }

    private void validateConfig() {
        final String message = "You can't set black and white lists for ";
        if (!nodeWhiteList.isEmpty() && !nodeBlackList.isEmpty()) {
            throw new RuntimeException(message + "nodes");
        }
        if (!relWhiteList.isEmpty() && !relBlackList.isEmpty()) {
            throw new RuntimeException(message + "rels");
        }
        if (!mapWhiteList.isEmpty() && !mapBlackList.isEmpty()) {
            throw new RuntimeException(message + "maps");
        }
    }

    public String getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public Map<String, List<String>> getNodeWhiteList() {
        return nodeWhiteList;
    }

    public Map<String, List<String>> getRelWhiteList() {
        return relWhiteList;
    }

    public Map<String, List<String>> getNodeBlackList() {
        return nodeBlackList;
    }

    public Map<String, List<String>> getRelBlackList() {
        return relBlackList;
    }

    public List<String> getMapWhiteList() {
        return mapWhiteList;
    }

    public List<String> getMapBlackList() {
        return mapBlackList;
    }
}
