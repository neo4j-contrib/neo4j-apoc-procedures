package apoc.es;

import apoc.Extended;
import apoc.load.LoadJsonUtils;
import apoc.result.MapResult;
import apoc.util.UrlResolver;
import apoc.util.Util;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 21.05.16
 */
@Extended
public class ElasticSearch {

    @Context
    public URLAccessChecker urlAccessChecker;
    
    private final static String fullQueryTemplate = "/%s/%s/%s?%s";

    // /{index}/{type}/_search?{query}
    private final static String fullQuerySearchTemplate = "/%s/%s/_search?%s";

    /**
     * With this pattern we can match both key:value params and key=value params
     */
    private final static Pattern KEY_VALUE = Pattern.compile("(.*)(:|=)(.*)");

    protected String getElasticSearchUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 9200).getUrl("es", hostOrKey);
    }

    /**
     * Get the full Elasticsearch url
     *
     * @param hostOrKey
     * @param index
     * @param type
     * @param id
     * @param query
     * @return
     */
    protected String getQueryUrl(String hostOrKey, String index, String type, String id, Object query) {
        return getElasticSearchUrl(hostOrKey) + formatQueryUrl(index, type, id, query);
    }

    /**
     * @param hostOrKey
     * @param index
     * @param type
     * @param query
     * @return
     */
    protected String getSearchQueryUrl(String hostOrKey, String index, String type, Object query) {
        return getElasticSearchUrl(hostOrKey) + formatSearchQueryUrl(index, type, query);
    }

    /**
     * @param index
     * @param type
     * @param query
     * @return
     */
    private String formatSearchQueryUrl(String index, String type, Object query) {
        String queryUrl = String.format(fullQuerySearchTemplate,
                index == null ? "_all" : index,
                type == null ? "_all" : type,
                toQueryParams(query));

        return queryUrl.endsWith("?") ? queryUrl.substring(0, queryUrl.length() - 1) : queryUrl;
    }

    /**
     * Format the query url template according to the parameters.
     * The format will be /{index}/{type}/{id}?{query} if query is not empty (or null) otherwise the format will be /{index}/{type}/{id}
     *
     * @param index
     * @param type
     * @param id
     * @param query
     * @return
     */
    private String formatQueryUrl(String index, String type, String id, Object query) {
        String queryUrl = String.format(fullQueryTemplate,
                index == null ? "_all" : index,
                type == null ? "_all" : type,
                id == null ? "" : id,
                toQueryParams(query));

        return queryUrl.endsWith("?") ? queryUrl.substring(0, queryUrl.length() - 1) : queryUrl;
    }

    /**
     * @param payload
     * @return
     */
    protected String toPayload(Object payload) {
        if (payload == null) return null;
        if (payload instanceof Map) return Util.toJson(payload);
        return payload.toString();
    }


    /**
     * @param query
     * @return
     */
    protected String toQueryParams(Object query) {
        if (query == null) return "";
        if (query instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) query;
            if (map.isEmpty()) return "";
            return map.entrySet().stream().map(e -> e.getKey() + "=" + Util.encodeUrlComponent(e.getValue().toString())).collect(Collectors.joining("&"));
        } else {
            // We have to encode only the values not the keys
            return Pattern.compile("&").splitAsStream(query.toString())
                    .map(KEY_VALUE::matcher)
                    .filter(Matcher::matches)
                    .map(matcher -> matcher.group(1) + matcher.group(2) + Util.encodeUrlComponent(matcher.group(3)))
                    .collect(Collectors.joining("&"));
        }
    }

    @Procedure
    @Description("apoc.es.stats(host-url-Key,$config) - elastic search statistics")
    public Stream<MapResult> stats(@Name("host") String hostOrKey, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        String url = getElasticSearchUrl(hostOrKey);
        return loadJsonStream(url + "/_stats", new ElasticSearchConfig(config), null);
    }

    @Procedure
    @Description("apoc.es.get(host-or-port,index-or-null,type-or-null,id-or-null,query-or-null,payload-or-null,$config) yield value - perform a GET operation on elastic search")
    public Stream<MapResult> get(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query, @Name("payload") Object payload,
                                 @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return loadJsonStream(getQueryUrl(hostOrKey, index, type, id, query), new ElasticSearchConfig(config), toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.query(host-or-port,index-or-null,type-or-null,query-or-null,payload-or-null,$config) yield value - perform a SEARCH operation on elastic search")
    public Stream<MapResult> query(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("query") Object query, @Name("payload") Object payload,
                                   @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return loadJsonStream(getSearchQueryUrl(hostOrKey, index, type, query), new ElasticSearchConfig(config), toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.getRaw(host-or-port,path,payload-or-null,$config) yield value - perform a raw GET operation on elastic search")
    public Stream<MapResult> getRaw(@Name("host") String hostOrKey, @Name("path") String suffix, @Name("payload") Object payload,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        String url = getElasticSearchUrl(hostOrKey);
        return loadJsonStream(url + "/" + suffix, new ElasticSearchConfig(config), toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.postRaw(host-or-port,path,payload-or-null,$config) yield value - perform a raw POST operation on elastic search")
    public Stream<MapResult> postRaw(@Name("host") String hostOrKey, @Name("path") String suffix, @Name("payload") Object payload, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        String url = getElasticSearchUrl(hostOrKey);
        return loadJsonStream(url + "/" + suffix, new ElasticSearchConfig(config, "POST"), toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.post(host-or-port,index-or-null,type-or-null,query-or-null,payload-or-null,$config) yield value - perform a POST operation on elastic search")
    public Stream<MapResult> post(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("query") Object query,
                                  @Name(value = "payload", defaultValue = "{}") Map<String,Object> payload,
                                  @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (payload == null)
        {
            payload = Collections.emptyMap();
        }
        return loadJsonStream(getQueryUrl(hostOrKey, index, type, null, query), new ElasticSearchConfig(config, "POST"), toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.put(host-or-port,index-or-null,type-or-null,id-or-null,query-or-null,payload-or-null,$config) yield value - perform a PUT operation on elastic search")
    public Stream<MapResult> put(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query,
                                 @Name(value = "payload", defaultValue = "{}") Map<String,Object> payload,
                                 @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (payload == null)
        {
            payload = Collections.emptyMap();
        }
        return loadJsonStream(getQueryUrl(hostOrKey, index, type, id, query), new ElasticSearchConfig(config, "PUT"), toPayload(payload));
    }

    private Stream<MapResult> loadJsonStream(@Name("url") Object url, ElasticSearchConfig conf, @Name("payload") String payload) {
        return LoadJsonUtils.loadJsonStream(url, conf.getHeaders(), payload, "", true, null, null, null, urlAccessChecker);
    }
}
