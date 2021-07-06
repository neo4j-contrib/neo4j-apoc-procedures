package apoc.nodes;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class NodesConfig {

    public static final String MAX_DEPTH_KEY = "maxDepth";
    
    private final int maxDepth;

    public NodesConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.maxDepth = Util.toInteger(config.getOrDefault(MAX_DEPTH_KEY, Integer.MAX_VALUE));
    }

    public int getMaxDepth() {
        return maxDepth;
    }
}
