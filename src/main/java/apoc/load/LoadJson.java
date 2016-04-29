package apoc.load;

import apoc.Description;
import apoc.convert.Json;
import apoc.result.ListResult;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.result.StringResult;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

public class LoadJson {

    @Context public GraphDatabaseService db;

    @Procedure
    @Description("apoc.load.jsonArray('url') YIELD value - load array from JSON URL (e.g. web-api) to import JSON as stream of values")
    public Stream<ObjectResult> jsonArray(@Name("url") String url) {
        try {
            Object value = Json.OBJECT_MAPPER.readValue(new URL(url), Object.class);
            if (value instanceof List) {
                return ((List)value).stream().map(ObjectResult::new);
            }
            throw new RuntimeException("Incompatible Type "+(value==null ? "null" : value.getClass()));
        } catch (IOException e) {
            throw new RuntimeException("Can't read url " + url + " as json", e);
        }
    }
    @Procedure
    @Description("apoc.load.json('http://example.com/map.json') YIELD value as person CREATE (p:Person) SET p = person - load from JSON URL (e.g. web-api) to import JSON as stream of values if the JSON was an array or a single value if it was a map")
    public Stream<MapResult> json(@Name("url") String url) {
        try {
            URL src = new URL( url );
            URLConnection con = src.openConnection();
            con.setConnectTimeout( 10_000 );
            con.setReadTimeout( 60_000 );

            InputStream stream = con.getInputStream();

            String encoding = con.getContentEncoding();
            if("gzip".equals( encoding )){
                stream = new GZIPInputStream( stream );
            }

            Object value = Json.OBJECT_MAPPER.readValue( stream, Object.class);
            if (value instanceof Map) {
                return Stream.of(new MapResult((Map)value));
            }
            if (value instanceof List) {
                return ((List)value).stream().map( (v) -> new MapResult((Map)v));
            }
            throw new RuntimeException("Incompatible Type "+(value==null ? "null" : value.getClass()));
        } catch (IOException e) {
            throw new RuntimeException("Can't read url " + url + " as json", e);
        }
    }
}
