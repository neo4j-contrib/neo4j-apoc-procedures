package apoc.es;

import apoc.ApocConfiguration;
import apoc.Description;
import apoc.load.LoadJson;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.URLEncoder;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;

/**
 * @author mh
 * @since 21.05.16
 */
public class ElasticSearch {

    public static final String DEFAULT_SCHEME = "http://";
    public static final String DEFAULT_HOST = "localhost";
    public static final String DEFAULT_PORT = ":9200";
    private static final String DEFAULT_URL = DEFAULT_SCHEME + DEFAULT_HOST +DEFAULT_PORT;

    @Procedure
    @Description("apoc.es.stats(host-url-Key) - elastic search statistics")
    public Stream<MapResult> stats(@Name("host") String hostOrKey) {
        String url = getESUrl(hostOrKey);
        return LoadJson.loadJsonStream(url+"/_stats",null,null);
    }

    @Procedure
    @Description("apoc.es.get(host-or-port,index-or-null,type-or-null,id-or-null,query-or-null,payload-or-null) yield value - perform a GET operation on elastic search")
    public Stream<MapResult> get(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query, @Name("payload") Object payload) {
        String url = getESUrl(hostOrKey);
        return LoadJson.loadJsonStream(String.format(url+"/%s/%s/%s?%s",
                index==null?"_all": index,
                type==null?"_all":type,
                id==null?"":id,
                toQueryParams(query)),null,toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.query(host-or-port,index-or-null,type-or-null,query-or-null,payload-or-null) yield value - perform a SEARCH operation on elastic search")
    public Stream<MapResult> query(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("query") Object query, @Name("payload") Object payload) {
        String url = getESUrl(hostOrKey);
        return LoadJson.loadJsonStream(String.format(url+"/%s/%s/%s?%s",
                index==null?"_all": index,
                type==null?"_all":type,
                "_search",
                toQueryParams(query)),null,toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.getRaw(host-or-port,path,payload-or-null) yield value - perform a raw GET operation on elastic search")
    public Stream<MapResult> getRaw(@Name("host") String hostOrKey, @Name("path") String suffix, @Name("payload") Object payload) {
        String url = getESUrl(hostOrKey);
        return LoadJson.loadJsonStream(url+"/"+suffix,null,toPayload(payload));
    }
    @Procedure
    @Description("apoc.es.postRaw(host-or-port,path,payload-or-null) yield value - perform a raw POST operation on elastic search")
    public Stream<MapResult> postRaw(@Name("host") String hostOrKey, @Name("path") String suffix, @Name("payload") Object payload) {
        String url = getESUrl(hostOrKey);
        return LoadJson.loadJsonStream(url+"/"+suffix,map("method","POST"),toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.post(host-or-port,index-or-null,type-or-null,query-or-null,payload-or-null) yield value - perform a POST operation on elastic search")
    public Stream<MapResult> post(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query, @Name("payload") Object payload) {
        String url = getESUrl(hostOrKey);
        return LoadJson.loadJsonStream(String.format(url+"/%s/%s/%s?%s",
                index==null?"_all": index,
                type==null?"_all":type,
                id==null?"":id,
                toQueryParams(query)),map("method","POST"),toPayload(payload));
    }
    @Procedure
    @Description("apoc.es.put(host-or-port,index-or-null,type-or-null,query-or-null,payload-or-null) yield value - perform a PUT operation on elastic search")
    public Stream<MapResult> put(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query, @Name("payload") Object payload) {
        String url = getESUrl(hostOrKey);
        return LoadJson.loadJsonStream(String.format(url+"/%s/%s/%s?%s",
                index==null?"_all": index,
                type==null?"_all":type,
                id==null?"":id,
                toQueryParams(query)),map("method","PUT"),toPayload(payload));
    }

    private String toPayload(Object payload) {
        if (payload==null) return null;
        if (payload instanceof Map) return Util.toJson(payload);
        return payload.toString();
    }

    private String toQueryParams(Object query) {
        if (query == null) return "";
        if (query instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) query;
            if (map.isEmpty()) return "";
            return Util.encodeUrlComponent(map.entrySet().stream().map(e -> e.getKey()+"="+ e.getValue()).collect(Collectors.joining("&")));
        }
        else return Util.encodeUrlComponent(query.toString());
    }

    private String getESUrl(String hostOrKey) {
        String url = getConfiguredUrl(hostOrKey+".");
        if (url!=null) return url;
        url = getConfiguredUrl("");
        if (url!=null) return url;
        url = resolveHost(hostOrKey);
        return url == null ? DEFAULT_URL : url;
    }

    private String getConfiguredUrl(String key) {
        String url = ApocConfiguration.get("es"+key+".url", null);
        if (url!=null) return url;
        String host = ApocConfiguration.get("es"+key+".host", null);
        return resolveHost(host);
    }

    private String resolveHost(String host) {
        if (host != null) {
            if (host.contains("//")) return host;
            if (host.contains(":")) return DEFAULT_SCHEME +host;
            return DEFAULT_SCHEME +host+ DEFAULT_PORT;
        }
        return null;
    }
}
