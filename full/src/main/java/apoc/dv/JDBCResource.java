package apoc.dv;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.MatchResult;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JDBCResource extends VirtualizedResource {

    private final List<String> parameters;

    public JDBCResource(String name, Map<String, Object> config) {
        super(name, parseQuery(config), "JDBC");
        parameters = PLACEHOLDER_PATTERN.matcher(query).results()
                .map(MatchResult::group)
                .map(String::trim)
                .map(param -> param.substring(1))
                .collect(Collectors.toList());
    }

    private static Map<String, Object> parseQuery(Map<String, Object> config) {
        String query = (String) config.get("query");
        if (StringUtils.isBlank(query) || countForQuestionMarks(query) > 0) return config;
        String queryParsed = PLACEHOLDER_PATTERN.matcher(query).replaceAll("?");
        config.put("queryParsed", queryParsed);
        return config;
    }

    @Override
    public long numOfQueryParams() {
        final long countForQuestionMarks = countForQuestionMarks(query);
        if (countForQuestionMarks > 0) {
            return countForQuestionMarks;
        }
        return countForQuestionMarks(queryParsed);
    }

    private static long countForQuestionMarks(String query) {
        return Stream.of(query.split("[ (),]"))
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> "?".equals(s))
                .count();
    }

    @Override
    public Pair<String, Map<String, Object>> getProcedureCallWithParams(Object queryParams) {
        final List<Object> list;
        if (parameters.isEmpty()) {
            list = (List<Object>) queryParams;
        } else {
            Map<String, Object> queryMap = (Map<String, Object>) queryParams;
            list = parameters.stream()
                .map(param -> queryMap.get(param))
                .collect(Collectors.toList());
        }
        return Pair.of("CALL apoc.load.jdbc($url, $query, $params, $config) YIELD row " +
                        "RETURN apoc.create.vNode($labels, row) AS node",
                Map.of("url", url, "query", queryParsed, "params", list, "labels", labels, "config", params));
    }

}
