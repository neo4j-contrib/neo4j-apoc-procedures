package apoc.diff;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class DiffConfig {

    private final boolean leftOnly;
    private final boolean rightOnly;
    private final boolean inCommon;
    private final boolean different;
    private final boolean findById;

    public DiffConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.leftOnly = Util.toBoolean(config.getOrDefault("leftOnly", true));
        this.rightOnly = Util.toBoolean(config.getOrDefault("rightOnly", true));
        this.inCommon = Util.toBoolean(config.getOrDefault("inCommon", true));
        this.different = Util.toBoolean(config.getOrDefault("different", true));
        this.findById = Util.toBoolean(config.getOrDefault("findById", false));
    }

    public boolean isLeftOnly() {
        return leftOnly;
    }

    public boolean isRightOnly() {
        return rightOnly;
    }

    public boolean isInCommon() {
        return inCommon;
    }

    public boolean isDifferent() {
        return different;
    }

    public boolean isFindById() {
        return findById;
    }
}
