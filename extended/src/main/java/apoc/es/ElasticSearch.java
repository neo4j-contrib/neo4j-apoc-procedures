package apoc.es;

import apoc.Extended;
import apoc.load.LoadJsonUtils;
import apoc.result.MapResult;
import apoc.util.Util;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 21.05.16
 */
@Extended
public class ElasticSearch {

    @Context
    public URLAccessChecker urlAccessChecker;
    
    /**
     * @param payload
     * @return
     */
    protected String toPayload(Object payload) {
        if (payload == null) return null;
        if (payload instanceof Map) return Util.toJson(payload);
        return payload.toString();
    }

    @Procedure
    @Description("apoc.es.stats(host-or-key,$config) - elastic search statistics")
    public Stream<MapResult> stats(@Name("host") String hostOrKey, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ElasticSearchConfig conf = new ElasticSearchConfig(config);
        String url = conf.getVersion().getElasticSearchUrl(hostOrKey);
        return loadJsonStream(url + "/_stats", conf, null);
    }

    @Procedure
    @Description("apoc.es.get(host-or-key,index-or-null,type-or-null,id-or-null,query-or-null,payload-or-null,$config) yield value - perform a GET operation on elastic search")
    public Stream<MapResult> get(@Name("hostOrKey") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query, @Name("payload") Object payload,
                                 @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ElasticSearchConfig conf = new ElasticSearchConfig(config);
        String queryUrl = conf.getVersion().getQueryUrl(hostOrKey, index, type, id, query);//.replace("mytype/", "");
        return loadJsonStream(queryUrl, conf, toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.query(host-or-key,index-or-null,type-or-null,query-or-null,payload-or-null,$config) yield value - perform a SEARCH operation on elastic search")
    public Stream<MapResult> query(@Name("hostOrKey") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("query") Object query, @Name("payload") Object payload,
                                   @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ElasticSearchConfig conf = new ElasticSearchConfig(config);
        String searchQueryUrl = conf.getVersion().getSearchQueryUrl(hostOrKey, index, type, query);//.replace("mytype/", "");

        return loadJsonStream(searchQueryUrl, conf, toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.getRaw(host-or-key,path,payload-or-null,$config) yield value - perform a raw GET operation on elastic search")
    public Stream<MapResult> getRaw(@Name("hostOrKey") String hostOrKey, @Name("path") String suffix, @Name("payload") Object payload,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ElasticSearchConfig conf = new ElasticSearchConfig(config);
        String url = conf.getVersion().getElasticSearchUrl(hostOrKey);
        return loadJsonStream(url + "/" + suffix, conf, toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.postRaw(host-or-key,path,payload-or-null,$config) yield value - perform a raw POST operation on elastic search")
    public Stream<MapResult> postRaw(@Name("hostOrKey") String hostOrKey, @Name("path") String suffix, @Name("payload") Object payload, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ElasticSearchConfig conf = new ElasticSearchConfig(config, "POST");
        String url = conf.getVersion().getElasticSearchUrl(hostOrKey);
        return loadJsonStream(url + "/" + suffix, conf, toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.post(host-or-key,index-or-null,type-or-null,query-or-null,payload-or-null,$config) yield value - perform a POST operation on elastic search")
    public Stream<MapResult> post(@Name("hostOrKey") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("query") Object query,
                                  @Name(value = "payload", defaultValue = "{}") Object payload,
                                  @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (payload == null)
        {
            payload = Collections.emptyMap();
        }
        ElasticSearchConfig conf = new ElasticSearchConfig(config, "POST");
        String queryUrl = conf.getVersion().getQueryUrl(hostOrKey, index, type, null, query);
        return loadJsonStream(queryUrl, conf, toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.put(host-or-key,index-or-null,type-or-null,id-or-null,query-or-null,payload-or-null,$config) yield value - perform a PUT operation on elastic search")
    public Stream<MapResult> put(@Name("hostOrKey") String hostOrKey, @Name("index") String index, @Name("type") String type, @Name("id") String id, @Name("query") Object query,
                                 @Name(value = "payload", defaultValue = "{}") Map<String,Object> payload,
                                 @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (payload == null)
        {
            payload = Collections.emptyMap();
        }

        ElasticSearchConfig conf = new ElasticSearchConfig(config, "PUT");
        String queryUrl = conf.getVersion().getQueryUrl(hostOrKey, index, type, id, query);
        return loadJsonStream(queryUrl, conf, toPayload(payload));
    }

    @Procedure
    @Description("apoc.es.delete(host-or-key,index-or-null,type-or-null,id-or-null,query-or-null,$config) yield value - perform a DELETE operation on elastic search")
    public Stream<MapResult> delete(@Name("hostOrKey") String hostOrKey,
                                    @Name("index") String index,
                                    @Name("type") String type,
                                    @Name("id") String id,
                                    @Name(value = "query", defaultValue = "null") Object query,
                                    @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        /* Conceptually payload should be null, but we have to put "" instead,
           as the `apoc.util.Util.writePayload` method has an `if (payload == null) return;`
           but we need to add the `con.setDoOutput(true);`, placed right after that condition.
           Otherwise, an error `Cannot write to a URLConnection if doOutput=false - call setDoOutput(true)` will be thrown
        */
        String payload = "";
        ElasticSearchConfig conf = new ElasticSearchConfig(config, "DELETE");
        String queryUrl = conf.getVersion().getQueryUrl(hostOrKey, index, type, id, query);
        return loadJsonStream(queryUrl, conf, payload);
    }

    private Stream<MapResult> loadJsonStream(@Name("url") Object url, ElasticSearchConfig conf, @Name("payload") String payload) {
        return LoadJsonUtils.loadJsonStream(url, conf.getHeaders(), payload, "", true, null, null, null, urlAccessChecker);
    }
}
