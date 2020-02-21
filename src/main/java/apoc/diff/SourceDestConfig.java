package apoc.diff;

import java.util.Collections;
import java.util.Map;

public class SourceDestConfig {
    public enum SourceDestConfigType { URL, DATABASE }

    static class TargetConfig {
        private final SourceDestConfigType type;
        private final String value;

        public TargetConfig(SourceDestConfigType type, String value) {
            this.type = type;
            this.value = value;
        }

        public SourceDestConfigType getType() {
            return type;
        }

        public String getValue() {
            return value;
        }
    }

    private final Map<String, Object> params;
    private final TargetConfig target;

    public SourceDestConfig(SourceDestConfigType type, String value, Map<String, Object> params) {
        this.target = new TargetConfig(type, value);
        this.params = params;
    }

    public TargetConfig getTarget() {
        return target;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public static SourceDestConfig fromMap(Map<String, Object> map) {
        map = map == null ? Collections.emptyMap() : map;
        if (map.isEmpty()) {
            return null;
        } else {
            Map<String, Object> target = (Map<String, Object>) map.getOrDefault("target", Collections.emptyMap());
            SourceDestConfigType type = SourceDestConfigType
                    .valueOf((String) target.getOrDefault("type", SourceDestConfigType.URL.toString()));
            return new SourceDestConfig(type,
                    (String) target.get("value"),
                    (Map<String, Object>) map.getOrDefault("params", Collections.emptyMap()));
        }
    }
}
