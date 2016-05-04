package apoc.util;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.procedure.Name;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

/**
 * @author mh
 * @since 04.05.16
 */
public class JsonUtil {
    public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static Object loadJson(@Name("url") String url) {
        try {
            URL src = new URL(url);
            URLConnection con = src.openConnection();
            con.setConnectTimeout(10_000);
            con.setReadTimeout(60_000);

            InputStream stream = con.getInputStream();

            String encoding = con.getContentEncoding();
            if ("gzip".equals(encoding)) {
                stream = new GZIPInputStream(stream);
            }
            if ("deflate".equals(encoding)) {
                stream = new DeflaterInputStream(stream);
            }

            return OBJECT_MAPPER.readValue(stream, Object.class);
        } catch (IOException e) {
            throw new RuntimeException("Can't read url " + url + " as json", e);
        }

    }
}
