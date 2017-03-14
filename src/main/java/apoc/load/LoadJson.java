package apoc.load;

import org.neo4j.procedure.Description;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.util.JsonUtil;
import apoc.util.MapUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.URI;
import java.util.*;
import java.util.stream.Stream;

public class LoadJson {

    private static final String AUTH_HEADER_KEY = "Authorization";

    @Context
    public GraphDatabaseService db;

    @SuppressWarnings("unchecked")
    @Procedure
    @Description("apoc.load.jsonArray('url') YIELD value - load array from JSON URL (e.g. web-api) to import JSON as stream of values")
    public Stream<ObjectResult> jsonArray(@Name("url") String url, @Name(value = "path",defaultValue = "") String path) {
        Object value = JsonUtil.loadJson(url,null,null,path);
        if (value instanceof List) {
            List list = (List) value;
            if (list.isEmpty()) return Stream.empty();
            if (list.get(0) instanceof Map) return list.stream().map(ObjectResult::new);
        }
        return Stream.of(new ObjectResult(value));
        // throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
    }

    @Procedure
    @Description("apoc.load.json('url') YIELD value -  import JSON as stream of values if the JSON was an array or a single value if it was a map")
    public Stream<MapResult> json(@Name("url") String url, @Name(value = "path",defaultValue = "") String path) {
        return jsonParams(url,null,null, path);
    }

    @SuppressWarnings("unchecked")
    @Procedure
    @Description("apoc.load.jsonParams('url',{header:value},payload) YIELD value - load from JSON URL (e.g. web-api) while sending headers / payload to import JSON as stream of values if the JSON was an array or a single value if it was a map")
    public Stream<MapResult> jsonParams(@Name("url") String url, @Name("headers") Map<String,Object> headers, @Name("payload") String payload, @Name(value = "path",defaultValue = "") String path) {
        return loadJsonStream(url, headers, payload, path);
    }

    public static Stream<MapResult> loadJsonStream(@Name("url") String url, @Name("headers") Map<String, Object> headers, @Name("payload") String payload) {
        return loadJsonStream(url, headers, payload, "" );
    }
    public static Stream<MapResult> loadJsonStream(@Name("url") String url, @Name("headers") Map<String, Object> headers, @Name("payload") String payload, String path) {
        headers = null != headers ? headers : new HashMap<>();
        headers.putAll(extractCredentialsIfNeeded(url));
        Object value = JsonUtil.loadJson(url,headers,payload, path);
        if (value instanceof Map) {
            return Stream.of(new MapResult((Map) value));
        }
        if (value instanceof List) {
            if (((List)value).isEmpty()) return Stream.empty();
            if (((List) value).get(0) instanceof Map)
                return ((List) value).stream().map((v) -> new MapResult((Map) v));
            return Stream.of(new MapResult(Collections.singletonMap("result",value)));
        }
        throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
    }

    private static Map<String, Object> extractCredentialsIfNeeded(String url) {
        try {
            URI uri = new URI(url);
            String authInfo = uri.getUserInfo();
            if (null != authInfo) {
                String[] parts = authInfo.split(":");
                if (2 == parts.length) {
                    String token = new String(Base64.getEncoder().encode(authInfo.getBytes()));
                    return MapUtil.map(AUTH_HEADER_KEY, "Basic " + token);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return Collections.emptyMap();
    }
}
