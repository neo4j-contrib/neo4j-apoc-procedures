package apoc.dv;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class VirtualizedResource {
    public static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$([^\\s]+)");
    public final String name;
    public final String url;
    public final String desc;
    public final List<String> labels;
    public final String query;
    public final List<String> params;
    public final String type;

    public VirtualizedResource(String name, Map<String, Object> config, String type) {
        this(Objects.requireNonNull(name, "Field `name` should be defined"),
                (String) Objects.requireNonNull(config.get("url"), "Field `url` in config should be defined"),
                (String) Objects.requireNonNull(config.get("desc"), "Field `desc` in config should be defined"),
                (List<String>) Objects.requireNonNull(config.get("labels"), "Field `labels` in config should be defined"),
                (String) Objects.requireNonNull(config.get("query"), "Field `query` in config should be defined"),
                PLACEHOLDER_PATTERN.matcher((String) config.get("query")).results()
                        .map(MatchResult::group)
                        .map(String::trim)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toList()),
                Objects.requireNonNull(type, "Field `type` should be defined")
        );
    }

    public VirtualizedResource(String name, String url, String desc, List<String> labels, String query, List<String> params, String type) {
        this.name = name;
        this.url = url;
        this.desc = desc;
        this.labels = labels;
        this.query = query;
        this.params = params;
        this.type = type;
        if (numOfQueryParams() <= 0) {
            throw new IllegalArgumentException("A virtualized resource must have at least one filter parameter.");
        }
    }

    public int numOfQueryParams() {
        return params.size();
    }

    protected abstract Map<String, Object> getProcedureParameters(Object queryParams, Map<String, Object> config);

    protected abstract String getProcedureCall(Map<String, Object> config);

    final public Pair<String, Map<String, Object>> getProcedureCallWithParams(Object queryParams, Map<String, Object> config) {
        validateQueryParams(queryParams);
        return Pair.of(getProcedureCall(config), getProcedureParameters(queryParams, config));
    }

    private void validateQueryParams(Object queryParams) {
        if (queryParams == null) {
            throw new IllegalArgumentException("Query Params cannot be null");
        }
        final int actualSize;
        if (queryParams instanceof Collection) {
            actualSize = CollectionUtils.size(queryParams);
        } else if (queryParams instanceof Map) {
            final Map<String, Object> parameterMap = (Map<String, Object>) queryParams;
            actualSize = MapUtils.size(parameterMap);
            Set<String> setParams = params.stream()
                    .collect(Collectors.toSet());
            final Set<String> actualParams = parameterMap.keySet().stream().map(p -> "$" + p).collect(Collectors.toSet());
            if (!actualParams.equals(setParams)) {
                List<String> sortedExpected = setParams.stream()
                        .sorted()
                        .collect(Collectors.toList());
                List<String> sortedActual = actualParams.stream()
                        .sorted()
                        .collect(Collectors.toList());
                throw new IllegalArgumentException(String.format("Expected query parameters are %s, actual are %s", sortedExpected, sortedActual));
            }
        } else {
            throw new IllegalArgumentException("Input params allowed are Maps and Lists");
        }
        final long expectedSize = numOfQueryParams();
        if (actualSize != expectedSize) {
            throw new IllegalArgumentException(String.format("Expected size is %d, actual is %d", expectedSize, actualSize));
        }
    }

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

    public VirtualizedResourceDTO toDTO() {
        return new VirtualizedResourceDTO(name, type, url, desc, labels, query, params);
    }

    public static class VirtualizedResourceDTO {
        public final String name;
        public final String type;
        public final String url;
        public final String desc;
        public final List<String> labels;
        public final String query;
        public final List<String> params;

        public VirtualizedResourceDTO(String name, String type, String url, String desc, List<String> labels, String query, List<String> params) {
            this.name = name;
            this.url = url;
            this.desc = desc;
            this.labels = labels;
            this.query = query;
            this.params = params;
            this.type = type;
        }
    }

}
