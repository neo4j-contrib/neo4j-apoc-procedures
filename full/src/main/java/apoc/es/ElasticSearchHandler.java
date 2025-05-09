package apoc.es;

import apoc.util.UrlResolver;
import apoc.util.Util;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

public abstract class ElasticSearchHandler {

    /**
     * With this pattern we can match both key:value params and key=value params
     */
    private static final Pattern KEY_VALUE = Pattern.compile("(.*)(:|=)(.*)");

    public String getElasticSearchUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 9200).getUrl("es", hostOrKey);
    }

    /**
     * @param query
     * @return
     */
    public String toQueryParams(Object query) {
        if (query == null) return "";
        if (query instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) query;
            if (map.isEmpty()) return "";
            return map.entrySet().stream()
                    .map(e -> e.getKey() + "="
                            + Util.encodeUrlComponent(e.getValue().toString()))
                    .collect(Collectors.joining("&"));
        } else {
            // We have to encode only the values not the keys
            return Pattern.compile("&")
                    .splitAsStream(query.toString())
                    .map(KEY_VALUE::matcher)
                    .filter(Matcher::matches)
                    .map(matcher -> matcher.group(1) + matcher.group(2) + Util.encodeUrlComponent(matcher.group(3)))
                    .collect(Collectors.joining("&"));
        }
    }

    /**
     * Get the full Elasticsearch url
     */
    public String getQueryUrl(String hostOrKey, String index, String type, String id, Object query) {
        return getElasticSearchUrl(hostOrKey) + formatQueryUrl(index, type, id, query);
    }

    /**
     * Get the full Elasticsearch search url
     */
    public String getSearchQueryUrl(String hostOrKey, String index, String type, Object query) {
        return getElasticSearchUrl(hostOrKey) + formatSearchQueryUrl(index, type, query);
    }

    /**
     * Format the Search API url template according to the parameters.
     */
    public abstract String formatSearchQueryUrl(String index, String type, Object query);

    /**
     * Format the query url template according to the parameters.
     * The format will be /{index}/{type}/{id}?{query} if query is not empty (or null) otherwise the format will be /{index}/{type}/{id}
     */
    public abstract String formatQueryUrl(String index, String type, String id, Object query);

    public enum Version {
        EIGHT(new Eight()),
        DEFAULT(new Default());

        private final ElasticSearchHandler handler;

        Version(ElasticSearchHandler handler) {
            this.handler = handler;
        }

        public ElasticSearchHandler get() {
            return handler;
        }
    }

    public static class Eight extends ElasticSearchHandler {

        @Override
        public String formatSearchQueryUrl(String index, String type, Object query) {

            String queryUrl = String.format("/%s/_search?%s", index == null ? "_all" : index, toQueryParams(query));

            return removeTerminalQuote(queryUrl);
        }

        @Override
        public String formatQueryUrl(String index, String type, String id, Object query) {

            String queryUrl = Arrays.asList(index, type, id).stream()
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.joining("/"));

            String queryParams = toQueryParams(query);
            queryParams = "".equals(queryParams) ? "" : ("?" + queryParams);

            return "/" + queryUrl + queryParams;
        }
    }

    public static class Default extends ElasticSearchHandler {

        private final String fullQueryTemplate = "/%s/%s/%s?%s";
        private final String fullQuerySearchTemplate = "/%s/%s/_search?%s";

        @Override
        public String formatSearchQueryUrl(String index, String type, Object query) {
            String queryUrl = String.format(
                    fullQuerySearchTemplate,
                    index == null ? "_all" : index,
                    type == null ? "_all" : type,
                    toQueryParams(query));

            return removeTerminalQuote(queryUrl);
        }

        @Override
        public String formatQueryUrl(String index, String type, String id, Object query) {
            String queryUrl = String.format(
                    fullQueryTemplate,
                    index == null ? "_all" : index,
                    type == null ? "_all" : type,
                    id == null ? "" : id,
                    toQueryParams(query));

            return removeTerminalQuote(queryUrl);
        }
    }

    @NotNull
    private static String removeTerminalQuote(String queryUrl) {
        return queryUrl.endsWith("?") ? queryUrl.substring(0, queryUrl.length() - 1) : queryUrl;
    }
}
