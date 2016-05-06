package apoc.load;

import apoc.Description;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.util.JsonUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LoadJson {

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.load.jsonArray('url') YIELD value - load array from JSON URL (e.g. web-api) to import JSON as stream of values")
    public Stream<ObjectResult> jsonArray(@Name("url") String url) {
        try {
            Object value = JsonUtil.OBJECT_MAPPER.readValue(new URL(url), Object.class);
            if (value instanceof List) {
                return ((List) value).stream().map(ObjectResult::new);
            }
            throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
        } catch (IOException e) {
            throw new RuntimeException("Can't read url " + url + " as json", e);
        }
    }

    @Procedure
    @Description("apoc.load.json('http://example.com/map.json') YIELD value as person CREATE (p:Person) SET p = person - load from JSON URL (e.g. web-api) to import JSON as stream of values if the JSON was an array or a single value if it was a map")
    public Stream<MapResult> json(@Name("url") String url) {
            Object value = JsonUtil.loadJson(url);
            if (value instanceof Map) {
                return Stream.of(new MapResult((Map) value));
            }
            if (value instanceof List) {
                return ((List) value).stream().map((v) -> new MapResult((Map) v));
            }
            throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
        }
}
