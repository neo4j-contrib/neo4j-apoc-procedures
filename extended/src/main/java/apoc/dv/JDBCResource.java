package apoc.dv;

import org.apache.commons.lang3.StringUtils;

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
        super(name,
                (String) config.get("url"),
                (String) config.get("desc"),
                (List<String>) config.get("labels"),
                (String) config.get("query"),
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
        return PLACEHOLDER_PATTERN.matcher(query).replaceAll("?");
    }

    @Override
    public int numOfQueryParams() {
        final int countForQuestionMarks = countForQuestionMarks(query);
        final int countForMapParameters = countForMapParameters(query);
        if (countForQuestionMarks > 0 && countForMapParameters > 0) {
            throw new IllegalArgumentException("The query is mixing parameters with `$` and `?` please use just one notation");
        }
        return countForQuestionMarks > 0 ? countForQuestionMarks : countForMapParameters;
    }

    private static int countForQuestionMarks(String query) {
        if (StringUtils.isBlank(query)) {
            return 0;
        }
        return (int) Stream.of(query.split("[ (),]"))
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> "?".equals(s))
                .count();
    }

    private static int countForMapParameters(String query) {
        if (StringUtils.isBlank(query)) {
            return 0;
        }
        return (int) PLACEHOLDER_PATTERN.matcher(query).results().count();
    }

    @Override
    protected Map<String, Object> getProcedureParameters(Object queryParams, Map<String, Object> config) {
        final long countForQuestionMarks = countForQuestionMarks(query);
        final List<Object> list;
        if (countForQuestionMarks > 0) {
            // get params from the list - queryParams is expected to be a list
            list = (List<Object>) queryParams;
        } else {
            // using $ we get params from the map, extract values based on each param name
            Map<String, Object> queryMap = (Map<String, Object>) queryParams;
            list = params.stream()
                    .map(param -> queryMap.get(param.substring(1)))
                    .collect(Collectors.toList());
        }
        return Map.of("url", url, "query", queryParsed, "params", list, "labels", labels, "config", config);
    }

    @Override
    protected String getProcedureCall(Map<String, Object> config) {
        return "CALL apoc.load.jdbc($url, $query, $params, $config) YIELD row " +
                "RETURN apoc.create.vNode($labels, row) AS node";
    }

}
