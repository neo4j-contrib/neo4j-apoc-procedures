package apoc.convert;

import java.net.URL;
import java.util.*;
import java.io.*;
import java.util.stream.*;

import apoc.Description;
import apoc.result.ListResult;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.result.StringResult;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import org.codehaus.jackson.map.ObjectMapper;

public class Json {

    public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    @Context public org.neo4j.graphdb.GraphDatabaseService db;

    @Procedure
    @Description("apoc.convert.toJson([1,2,3]) or toJson({a:42,b:\"foo\",c:[1,2,3]})")
    public Stream<StringResult> toJson(@Name("value") Object value) {
        try {
            return Stream.of(new StringResult(OBJECT_MAPPER.writeValueAsString(value)));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure
    @PerformsWrites
    @Description("apoc.json.setJsonProperty(node,key,complexValue) - sets value serialized to JSON as property with the given name on the node")
    public void setJsonProperty(@Name("node") Node node, @Name("key") String key, @Name("value") Object value) {
        try {
            node.setProperty(key,OBJECT_MAPPER.writeValueAsString(value));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure
    @Description("apoc.json.getJsonProperty(node,key) - converts serialized JSON in property to structure again")
    public Stream<ObjectResult> getJsonProperty(@Name("node") Node node, @Name("key") String key) {
        String value = (String)node.getProperty(key, null);
        try {
            return Stream.of(new ObjectResult(OBJECT_MAPPER.readValue(value, Object.class)));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure
    @Description("apoc.convert.fromJsonMap('{\"a\":42,\"b\":\"foo\",\"c\":[1,2,3]}')")
    public Stream<MapResult> fromJsonMap(@Name("map") String value) {
        try {
            return Stream.of(new MapResult(OBJECT_MAPPER.readValue(value, Map.class)));
        } catch (IOException e) {
            throw new RuntimeException("Can't deserialize to Map:\n"+value,e);
        }
    }

    @Procedure
    @Description("apoc.convert.fromJsonList('[1,2,3]')")
    public Stream<ListResult> fromJsonList(@Name("list") String value) {
        try {
            return Stream.of(new ListResult(OBJECT_MAPPER.readValue(value, List.class)));
        } catch (IOException e) {
            throw new RuntimeException("Can't deserialize to List:\n"+value,e);
        }
    }
}
