package apoc.util;

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

    public static Object loadJson(@Name("url") String url, Map<String,Object> headers, String payload) {
        try {
            InputStream stream = Util.openInputStream(url, headers, payload);
            String data = new Scanner(stream, "UTF-8").useDelimiter("\\Z").next();
            return OBJECT_MAPPER.readValue(data, Object.class);
        } catch (EOFException eof) {
            return null;
        } catch (IOException e) {
            throw new RuntimeException("Can't read url " + url + " as json", e);
        }
    }
    public static Object loadJson(@Name("url") String url) {
        return loadJson(url,null,null);
    }

}
