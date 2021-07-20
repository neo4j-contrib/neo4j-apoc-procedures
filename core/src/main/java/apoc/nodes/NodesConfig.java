package apoc.nodes;

import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;

public class NodesConfig {

    public static final String MAX_DEPTH_KEY = "maxDepth";
    public static final String REL_TYPE_KEY = "relType";
    
    private final int maxDepth;
    private final String relType;

    public NodesConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.maxDepth = Util.toInteger(config.getOrDefault(MAX_DEPTH_KEY, Integer.MAX_VALUE));
        this.relType = (String) config.getOrDefault(REL_TYPE_KEY, StringUtils.EMPTY);
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public String getRelType() {
        return relType;
    }
}
