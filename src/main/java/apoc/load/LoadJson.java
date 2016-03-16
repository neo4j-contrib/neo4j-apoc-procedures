package apoc.load;

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
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LoadJson {

    @Context public GraphDatabaseService db;

    @Procedure
    public Stream<ObjectResult> json(@Name("url") String url) {
        try {
            Object value = Json.OBJECT_MAPPER.readValue(new URL(url), Object.class);
                if (value instanceof Iterable) {
                return StreamSupport.stream(((Iterable<Object>)value).spliterator(),false).<ObjectResult>map(ObjectResult::new);
            }
            if (value instanceof Map) {
                return Stream.of(new ObjectResult(value));
            }
            throw new RuntimeException("Incompatible Type "+(value==null ? "null" : value.getClass()));
        } catch (IOException e) {
            throw new RuntimeException("Can't read url " + url + " as json", e);
        }
    }
}
