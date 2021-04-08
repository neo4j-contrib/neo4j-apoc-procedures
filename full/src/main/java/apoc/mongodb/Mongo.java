package apoc.mongodb;

import apoc.result.MapResult;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static apoc.mongodb.MongoDB.executeMongoQuery;

public class Mongo {

    @Context
    public Log log;
    @Procedure
    @Description("apoc.mongodb.get(host-or-key,db,collection,query,[compatibleValues=false|true],skip-or-null,limit-or-null,[extractReferences=false|true],[objectIdAsMap=true|false],[useExtendedJson=true|false]) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> get(@Name("host") String hostOrKey,
//                                 @Name("db") String db,
                                 @Name("collection") String collection,
//                                 @Name("query") Map<String, Object> query,
//                                 @Name(value = "compatibleValues", defaultValue = "false") boolean compatibleValues,
//                                 @Name(value = "skip", defaultValue = "0") Long skip,
//                                 @Name(value = "limit", defaultValue = "0") Long limit,
//                                 @Name(value = "extractReferences", defaultValue = "false") boolean extractReferences,
//                                 @Name(value = "objectIdAsMap", defaultValue = "true") boolean objectIdAsMap,
//                                 @Name(value = "useExtendedJson", defaultValue = "false") boolean useExtendedJson
                                 @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        MongoDbConfig conf = new MongoDbConfig(config);
        return executeMongoQuery(hostOrKey, db, collection, compatibleValues,
                extractReferences, objectIdAsMap, coll -> coll.all(query, skip, limit, useExtendedJson).map(MapResult::new),
                e -> log.error("apoc.mongodb.get - hostOrKey = [" + hostOrKey + "], db = [" + db + "], collection = [" + collection + "], query = [" + query + "], compatibleValues = [" + compatibleValues + "], skip = [" + skip + "], limit = [" + limit + "]", e));
    }
}
