package apoc.model;

import apoc.util.Util;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ModelConfig {

    private final boolean write;

    private final String schema;

    private final List<String> tables;
    private final List<String> views;
    private final List<String> columns;

    public ModelConfig(Map<String, Object> config) {
        this.write = Util.toBoolean(config.getOrDefault("write",false));
        Map<String, List<String>> filters = (Map<String, List<String>>) config.getOrDefault("filters", Collections.emptyMap());
        this.tables = toPatternList(filters.getOrDefault("tables", Collections.emptyList()));
        this.views = toPatternList(filters.getOrDefault("views", Collections.emptyList()));
        this.columns = toPatternList(filters.getOrDefault("columns", Collections.emptyList()));
        this.schema = config.getOrDefault("schema", "").toString();
    }


    private List<String> toPatternList(List<String> patterns) {
        return patterns
                .stream()
                .collect(Collectors.toList());
    }

    public List<String> getViews() {
        return views;
    }

    public List<String> getColumns() {
        return columns;
    }

    public boolean isWrite() {
        return write;
    }

    public List<String> getTables() {
        return tables;
    }

    public String getSchema() {
        return schema;
    }
}
