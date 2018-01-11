package apoc.util;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingIterator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.procedure.Name;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 04.05.16
 */
public class JsonUtil {
    public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static Object TOMB = new Object();
    private static final Configuration JSON_PATH_CONFIG = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS).build();
    static {
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    static class NonClosingStream extends FilterInputStream {

        protected NonClosingStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static Stream<Object> loadJson(String url, Map<String,Object> headers, String payload) {
        return loadJson(url,headers,payload,"", true);
    }
    public static Stream<Object> loadJson(String url, Map<String,Object> headers, String payload, String path, boolean failOnError) {
        try {
            FileUtils.checkReadAllowed(url);
            url = FileUtils.changeFileUrlIfImportDirectoryConstrained(url);
            InputStream input = Util.openInputStream(url, headers, payload);
            JsonParser parser = OBJECT_MAPPER.getJsonFactory().createJsonParser(input);
            MappingIterator<Object> it = OBJECT_MAPPER.readValues(parser, Object.class);
            Stream<Object> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
            return (path==null||path.isEmpty()) ? stream  : stream.map((value) -> JsonPath.parse(value,JSON_PATH_CONFIG).read(path));
        } catch (IOException e) {
            String u = Util.cleanUrl(url);
            if(!failOnError)
                return Stream.of();
            else
                throw new RuntimeException("Can't read url " + u + " as json: "+e.getMessage(), e);
        }
    }

    public static Stream<Object> loadJson(@Name("url") String url) {
        return loadJson(url,null,null,"", true);
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
