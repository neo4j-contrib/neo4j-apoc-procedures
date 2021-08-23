package apoc.nodes;

import apoc.util.Util;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NodesConfig {

    public static final String MAX_DEPTH_KEY = "maxDepth";
    public static final String REL_TYPE_KEY = "relTypes";
    
    private final int maxDepth;
    private final List<String> relTypes;

    public NodesConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.maxDepth = Util.toInteger(config.getOrDefault(MAX_DEPTH_KEY, Integer.MAX_VALUE));
        this.relTypes = (List<String>) config.getOrDefault(REL_TYPE_KEY, Collections.emptyList());
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public List<String> getRelTypes() {
        return relTypes;
    }
}
