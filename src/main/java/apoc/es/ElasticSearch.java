package apoc.es;

import apoc.Description;
import apoc.load.LoadJson;
import apoc.result.MapResult;
import apoc.util.UrlResolver;
import apoc.util.Util;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;

/**
 * @author mh
 * @since 21.05.16
 */
public class ElasticSearch {

    private String getElasticSearchUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 9200).getUrl("es", hostOrKey);
    }

    @Procedure
    @Description("apoc.es.stats(host-url-Key) - elastic search statistics")
    public Stream<MapResult> stats(@Name("host") String hostOrKey) {
        String url = getElasticSearchUrl(hostOrKey);
        return LoadJson.loadJsonStream(url+"/_stats",null,null);
    }

    @Procedure
    @Description("apoc.es.get(host-or-port,index-or-null,type-or-null,id-or-null,query-or-null,payload-or-null) yield value - perform a GET operation on elastic search")
    public Stream<MapResult> get(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query, @Name("payload") Object payload) {
        String url = getElasticSearchUrl(hostOrKey);
        return LoadJson.loadJsonStream(String.format(url+"/%s/%s/%s?%s",
                index==null?"_all": index,
                type==null?"_all":type,
                id==null?"":id,
                toQueryParams(query)),null,toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.query(host-or-port,index-or-null,type-or-null,query-or-null,payload-or-null) yield value - perform a SEARCH operation on elastic search")
    public Stream<MapResult> query(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("query") Object query, @Name("payload") Object payload) {
        String url = getElasticSearchUrl(hostOrKey);
        return LoadJson.loadJsonStream(String.format(url+"/%s/%s/%s?%s",
                index==null?"_all": index,
                type==null?"_all":type,
                "_search",
                toQueryParams(query)),null,toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.getRaw(host-or-port,path,payload-or-null) yield value - perform a raw GET operation on elastic search")
    public Stream<MapResult> getRaw(@Name("host") String hostOrKey, @Name("path") String suffix, @Name("payload") Object payload) {
        String url = getElasticSearchUrl(hostOrKey);
        return LoadJson.loadJsonStream(url+"/"+suffix,null,toPayload(payload));
    }
    @Procedure
    @Description("apoc.es.postRaw(host-or-port,path,payload-or-null) yield value - perform a raw POST operation on elastic search")
    public Stream<MapResult> postRaw(@Name("host") String hostOrKey, @Name("path") String suffix, @Name("payload") Object payload) {
        String url = getElasticSearchUrl(hostOrKey);
        return LoadJson.loadJsonStream(url+"/"+suffix,map("method","POST"),toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.post(host-or-port,index-or-null,type-or-null,query-or-null,payload-or-null) yield value - perform a POST operation on elastic search")
    public Stream<MapResult> post(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query, @Name("payload") Object payload) {
        String url = getElasticSearchUrl(hostOrKey);
        return LoadJson.loadJsonStream(String.format(url+"/%s/%s/%s?%s",
                index==null?"_all": index,
                type==null?"_all":type,
                id==null?"":id,
                toQueryParams(query)),map("method","POST"),toPayload(payload));
    }
    @Procedure
    @Description("apoc.es.put(host-or-port,index-or-null,type-or-null,query-or-null,payload-or-null) yield value - perform a PUT operation on elastic search")
    public Stream<MapResult> put(@Name("host") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query, @Name("payload") Object payload) {
        String url = getElasticSearchUrl(hostOrKey);
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
}
