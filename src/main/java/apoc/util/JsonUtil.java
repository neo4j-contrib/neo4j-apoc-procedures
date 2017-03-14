package apoc.util;

import apoc.ApocConfiguration;
import apoc.export.util.FileUtils;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.procedure.Name;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Scanner;

/**
 * @author mh
 * @since 04.05.16
 */
public class JsonUtil {
    public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Configuration JSON_PATH_CONFIG = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS).build();
    static {
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

    }


    public static Object loadJson(@Name("url") String url, Map<String,Object> headers, String payload) {
        return loadJson(url,headers,payload,"");
    }
    public static Object loadJson(@Name("url") String url, Map<String,Object> headers, String payload, String path) {
        try {
            FileUtils.checkReadAllowed(url);
            InputStream stream = Util.openInputStream(url, headers, payload);
            String data = new Scanner(stream, "UTF-8").useDelimiter("\\Z").next();
            if (path==null || path.isEmpty()) {
                return OBJECT_MAPPER.readValue(data, Object.class);
            }
            return JsonPath.parse(data,JSON_PATH_CONFIG).read(path);
        } catch (EOFException eof) {
            return null;
        } catch (IOException e) {
            String u = Util.cleanUrl(url);
            throw new RuntimeException("Can't read url " + u + " as json: "+e.getMessage(), e);
        }
    }

    public static Object loadJson(@Name("url") String url) {
        return loadJson(url,null,null,"");
    }

    public static <T> T parse(String json, String path, Class<T> type) {
        if (json==null || json.isEmpty()) return null;
        try {
            if (path == null || path.isEmpty()) return OBJECT_MAPPER.readValue(json, type);
            return JsonPath.parse(json,JSON_PATH_CONFIG).read(path,type);
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + json + " to "+type.getSimpleName()+" with path "+path, e);
        }
    }
}
