package apoc.dv;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.MatchResult;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class JDBCResource extends VirtualizedResource {

    private final String queryParsed;

    public JDBCResource(String name, Map<String, Object> config) {
        super(Objects.requireNonNull(name, "Field `name` should be defined"),
                (String) Objects.requireNonNull(config.get("url"), "Field `url` in config should be defined"),
                (String) Objects.requireNonNull(config.get("desc"), "Field `desc` in config should be defined"),
                (List<String>) Objects.requireNonNull(config.get("labels"), "Field `labels` in config should be defined"),
                (String) Objects.requireNonNull(config.get("query"), "Field `query` in config should be defined"),
                getParameters(config),
                "JDBC");
        this.queryParsed = parseQuery(config);
    }

    private static List<String> getParameters(Map<String, Object> config) {
        final String query = (String) config.get("query");
        final long questionMarks = countForQuestionMarks(query);
        if (questionMarks > 0) {
            return LongStream.range(0, questionMarks)
                    .mapToObj(i -> "?")
                    .collect(Collectors.toList());
        }
        return PLACEHOLDER_PATTERN.matcher(query).results()
                .map(MatchResult::group)
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }

    private static String parseQuery(Map<String, Object> config) {
        String query = (String) config.get("query");
        return parseQuery(query);
    }

    private static String parseQuery(String query) {
        final long questionMarks = countForQuestionMarks(query);
        if (questionMarks > 0) {
            return query;
        }
        return PLACEHOLDER_PATTERN.matcher(query).replaceAll("?");
    }

    @Override
    public long numOfQueryParams() {
        final long countForQuestionMarks = countForQuestionMarks(query);
        if (countForQuestionMarks > 0) {
            return countForQuestionMarks;
        }
        return countForQuestionMarks(parseQuery(query));
    }

    private static long countForQuestionMarks(String query) {
        if (StringUtils.isBlank(query)) {
            return 0;
        }
        return Stream.of(query.split("[ (),]"))
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> "?".equals(s))
                .count();
    }

    @Override
    public Pair<String, Map<String, Object>> getProcedureCallWithParams(Object queryParams, Map<String, Object> config) {
        final long countForQuestionMarks = countForQuestionMarks(query);
        final List<Object> list;
        if (countForQuestionMarks > 0) {
            list = (List<Object>) queryParams;
        } else {
            Map<String, Object> queryMap = (Map<String, Object>) queryParams;
            list = params.stream()
                .map(param -> queryMap.get(param.substring(1)))
                .collect(Collectors.toList());
        }
        return Pair.of("CALL apoc.load.jdbc($url, $query, $params, $config) YIELD row " +
                        "RETURN apoc.create.vNode($labels, row) AS node",
                Map.of("url", url, "query", queryParsed, "params", list, "labels", labels, "config", config));
    }

}
