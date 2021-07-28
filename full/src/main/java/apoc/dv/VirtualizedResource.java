package apoc.dv;

import org.neo4j.internal.helpers.collection.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public abstract class VirtualizedResource {
    public static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$([^\\s]+)");
    public final String name;
    public final String url;
    public final String desc;
    public final List<String> labels;
    public final String query;
    public final String queryParsed;
    public final Map<String, Object> params;
    public final String type;

    public VirtualizedResource(String name, Map<String, Object> config, String type) {
        this.name = Objects.requireNonNull(name, "Field `name` should be defined");
        this.type = Objects.requireNonNull(type, "Field `type` should be defined");
        url = (String) Objects.requireNonNull(config.get("url"), "Field `url` in config should be defined");
        query = (String) Objects.requireNonNull(config.get("query"), "Field `query` in config should be defined");
        queryParsed = (String) config.getOrDefault("queryParsed", config.get("query"));
        desc = (String) Objects.requireNonNull(config.get("desc"), "Field `desc` in config should be defined");
        labels = (List<String>) Objects.requireNonNull(config.get("labels"), "Field `labels` in config should be defined");
        params = (Map<String, Object>) config.getOrDefault("params", Map.of());
        if (numOfQueryParams() <= 0) {
            throw new IllegalArgumentException("A virtualized resource must have at least one filter parameter.");
        }
    }

    public long numOfQueryParams() {
        return PLACEHOLDER_PATTERN.matcher(query).results().count();
    }

    public abstract Pair<String, Map<String, Object>> getProcedureCallWithParams(Object queryParams);

    public static VirtualizedResource from(String name, Map<String, Object> config) {
        String type = config.get("type").toString();
        switch (type) {
            case "CSV":
                return new CSVResource(name, config);
            case "JDBC":
                return new JDBCResource(name, config);
            default:
                throw new UnsupportedOperationException("Supported CSV/JDBC");
        }
    }

}
