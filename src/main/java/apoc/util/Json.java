package apoc.util;

import java.net.URL;
import java.util.*;
import java.io.*;
import java.util.stream.*;

import apoc.util.result.ListResult;
import apoc.util.result.MapResult;
import apoc.util.result.ObjectResult;
import apoc.util.result.StringResult;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import org.codehaus.jackson.map.ObjectMapper;

public class Json {

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    @Context public org.neo4j.graphdb.GraphDatabaseService db;

    @Procedure
    public Stream<StringResult> toJson(@Name("value") Object value) {
        try {
            return Stream.of(new StringResult(OBJECT_MAPPER.writeValueAsString(value)));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure
    @PerformsWrites
    public void setJsonProperty(@Name("node") Node node, @Name("key") String key, @Name("value") Object value) {
        try {
            node.setProperty(key,OBJECT_MAPPER.writeValueAsString(value));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure
    public Stream<ObjectResult> getJsonProperty(@Name("node") Node node, @Name("key") String key) {
        String value = (String)node.getProperty(key, null);
        try {
            return Stream.of(new ObjectResult(OBJECT_MAPPER.readValue(value, Object.class)));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure
    public Stream<MapResult> fromJsonMap(@Name("map") String value) {
        try {
            return Stream.of(new MapResult(OBJECT_MAPPER.readValue(value, Map.class)));
        } catch (IOException e) {
            throw new RuntimeException("Can't deserialize to Map:\n"+value,e);
        }
    }

    @Procedure
    public Stream<ListResult> fromJsonList(@Name("list") String value) {
        try {
            return Stream.of(new ListResult(OBJECT_MAPPER.readValue(value, List.class)));
        } catch (IOException e) {
            throw new RuntimeException("Can't deserialize to List:\n"+value,e);
        }
    }

    @Procedure
    public Stream<ObjectResult> loadJson(@Name("url") String url) {
        try {
            Object value = OBJECT_MAPPER.readValue(new URL(url), Object.class);
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

//    @Procedure
//    public Stream<ObjectResult> test() {
//        return Stream.of(new ObjectResult(Collections.singletonMap("foo",42)));
//    }
}
