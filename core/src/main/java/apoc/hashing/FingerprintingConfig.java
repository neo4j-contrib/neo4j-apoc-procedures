package apoc.hashing;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FingerprintingConfig {
    private final String digestAlgorithm;
    private final Map<String, List<String>> nodeAllowMap;
    private final Map<String, List<String>> relAllowMap;

    private final Map<String, List<String>> nodeDisallowMap;
    private final Map<String, List<String>> relDisallowMap;

    private final List<String> mapAllowList;
    private final List<String> mapDisallowList;


    public FingerprintingConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.digestAlgorithm = (String) config.getOrDefault("digestAlgorithm", "MD5");

        this.nodeAllowMap = (Map<String, List<String>>) config.getOrDefault("nodeAllowMap", Collections.emptyMap());
        this.relAllowMap = (Map<String, List<String>>) config.getOrDefault("relAllowMap", Collections.emptyMap());
        this.nodeDisallowMap = (Map<String, List<String>>) config.getOrDefault("nodeDisallowMap", Collections.emptyMap());
        this.relDisallowMap = (Map<String, List<String>>) config.getOrDefault("relDisallowMap", Collections.emptyMap());
        this.mapAllowList = (List<String>) config.getOrDefault("mapAllowList", Collections.emptyList());
        this.mapDisallowList = (List<String>) config.getOrDefault("mapDisallowList", config.getOrDefault("propertyExcludes", Collections.emptyList()));

        validateConfig();
    }

    private void validateConfig() {
        final String message = "You can't set allow and disallow lists for ";
        if (!nodeAllowMap.isEmpty() && !nodeDisallowMap.isEmpty()) {
            throw new RuntimeException(message + "nodes");
        }
        if (!relAllowMap.isEmpty() && !relDisallowMap.isEmpty()) {
            throw new RuntimeException(message + "rels");
        }
        if (!mapAllowList.isEmpty() && !mapDisallowList.isEmpty()) {
            throw new RuntimeException(message + "maps");
        }
    }

    public String getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public Map<String, List<String>> getNodeAllowMap() {
        return nodeAllowMap;
    }

    public Map<String, List<String>> getRelAllowMap() {
        return relAllowMap;
    }

    public Map<String, List<String>> getNodeDisallowMap() {
        return nodeDisallowMap;
    }

    public Map<String, List<String>> getRelDisallowMap() {
        return relDisallowMap;
    }

    public List<String> getMapAllowList() {
        return mapAllowList;
    }

    public List<String> getMapDisallowList() {
        return mapDisallowList;
    }
}