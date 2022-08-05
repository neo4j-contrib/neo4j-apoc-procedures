package apoc.load;

import apoc.result.MapResult;
import apoc.util.JsonUtil;
import apoc.util.Util;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.procedure.Name;

public class LoadJsonUtils {
    public static Stream<MapResult> loadJsonStream(@Name("url") Object url, @Name("headers") Map<String, Object> headers, @Name("payload") String payload) {
        return loadJsonStream(url, headers, payload, "", true, null, null);
    }
    public static Stream<MapResult> loadJsonStream(@Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary, @Name("headers") Map<String, Object> headers, @Name("payload") String payload, String path, boolean failOnError, String compressionAlgo, List<String> pathOptions) {
        if (urlOrKeyOrBinary instanceof String) {
            headers = null != headers ? headers : new HashMap<>();
            headers.putAll(Util.extractCredentialsIfNeeded((String) urlOrKeyOrBinary, failOnError));
        }
        Stream<Object> stream = JsonUtil.loadJson(urlOrKeyOrBinary,headers,payload, path, failOnError, compressionAlgo, pathOptions);
        return stream.flatMap((value) -> {
            if (value instanceof Map) {
                return Stream.of(new MapResult((Map) value));
            }
            if (value instanceof List) {
                if (((List)value).isEmpty()) return Stream.empty();
                if (((List) value).get(0) instanceof Map)
                    return ((List) value).stream().map((v) -> new MapResult((Map) v));
                return Stream.of(new MapResult(Collections.singletonMap("result",value)));
            }
            if(!failOnError)
                throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
            else
                return Stream.of(new MapResult(Collections.emptyMap()));
        });
    }
}
