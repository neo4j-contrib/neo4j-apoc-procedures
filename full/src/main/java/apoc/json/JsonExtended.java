package apoc.json;

import apoc.Extended;
import apoc.result.StringResult;
import apoc.util.JsonUtil;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.stream.Stream;


@Extended
public class JsonExtended {

    @Context
    public org.neo4j.graphdb.GraphDatabaseService db;

    @Procedure
    @Description("apoc.json.validate('{json}' [,'json-path' , 'path-options']) - to check if the json is correct (returning an empty result) or not")
    public Stream<StringResult> validate(@Name("json") String json, @Name(value = "path",defaultValue = "$") String path, @Name(value = "pathOptions", defaultValue = "null") List<String> pathOptions) {
        return ((List<String>) JsonUtil.parse(json, path, Object.class, pathOptions, true))
                .stream()
                .map(StringResult::new);
    }
    
}
