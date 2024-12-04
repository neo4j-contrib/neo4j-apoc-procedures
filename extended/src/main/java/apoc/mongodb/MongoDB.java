package apoc.mongodb;

import apoc.Extended;
import apoc.result.MapResultExtended;
import apoc.util.UrlResolver;
import org.bson.types.ObjectId;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static apoc.mongodb.MongoDBUtils.getMongoConfig;

/*
 also see https://docs.mongodb.com/ecosystem/tools/http-interfaces/#simple-rest-api
// Load courses from MongoDB
CALL apoc.load.json('http://127.0.0.1:28017/course_catalog/course/') YIELD value
UNWIND value.rows AS course
// Create Course nodes
MERGE (c:Course {_id: course._id['$oid']})
ON CREATE SET c.name = course.name
WITH course, c
// Create Category nodes and connect to Course
FOREACH (category IN course.categories |
    MERGE (cat:Category {_id: category.id})
    ON CREATE SET cat.name = category.name
    MERGE (c)-[:HAS_CATEGORY]->(cat)
)
WITH course, c
// Create University nodes and connect to Course
UNWIND course.universities AS univ
WITH c, univ WHERE univ.id IS NOT NULL
MERGE (u:University {id: univ.id})
ON CREATE SET u.location  = univ.location,
              u.shortName = univ.shortName,
              u.url       = univ.website
MERGE (c)-[:OFFERED_BY]->(u)
 */
@Extended
public class MongoDB {

    @Context
    public Log log;

    private MongoDbCollInterface getColl(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection,
                         boolean compatibleValues, boolean extractReferences, boolean objectIdAsMap) {
        String url = getMongoDBUrl(hostOrKey);
        return MongoDbCollInterface.Factory.create(url, db, collection, compatibleValues, extractReferences, objectIdAsMap);
    }

    @Procedure("apoc.mongodb.get.byObjectId")
    @Description("apoc.mongodb.get.byObjectId(hostOrKey, db, collection, objectIdValue, config(default:{})) - get the document by Object id value")
    public Stream<MapResultExtended> byObjectId(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection, @Name("objectIdValue") String objectIdValue, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        MongoDbConfig conf = getMongoConfig(config);

        return executeMongoQuery(hostOrKey, db, collection, conf.isCompatibleValues(), conf.isExtractReferences(), conf.isObjectIdAsMap(),
                coll -> {
                    Map<String, Object> result = coll.first(Map.of(conf.getIdFieldName(), new ObjectId(objectIdValue)));
                    return result == null || result.isEmpty() ? Stream.empty() : Stream.of(new MapResultExtended(result));
                },
                e -> log.error("apoc.mongodb.get.byObjectId - hostOrKey = [" + hostOrKey + "], db = [" + db + "], collection = [" + collection + "], objectIdValue = [" + objectIdValue + "]",e));
    }

    private String getMongoDBUrl(String hostOrKey) {
        return new UrlResolver("mongodb", "localhost", 27017).getUrl("mongodb", hostOrKey);
    }

    private  <T> Stream<T> executeMongoQuery(String hostOrKey, String db, String collection, boolean compatibleValues,
                                            boolean extractReferences, boolean objectIdAsMap, Function<MongoDbCollInterface, Stream<T>> execute, Consumer<Exception> onError) {
        MongoDbCollInterface coll = null;
        try {
            coll = getColl(hostOrKey, db, collection, compatibleValues, extractReferences, objectIdAsMap);
            return execute.apply(coll).onClose(coll::safeClose);
        } catch (Exception e) {
            if (coll != null) {
                coll.safeClose();
            }
            onError.accept(e);
            throw new RuntimeException("Error during connection", e);
        }
    }
}
