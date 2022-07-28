package apoc.mongodb;

import apoc.Extended;
import apoc.result.MapResult;
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

import static apoc.mongodb.MongoDBUtils.getMongoColl;
import static apoc.mongodb.MongoDBUtils.Coll;

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

    private Coll getColl(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection,
                         boolean compatibleValues, boolean extractReferences, boolean objectIdAsMap) {
        String url = getMongoDBUrl(hostOrKey);
        return Coll.Factory.create(url, db, collection, compatibleValues, extractReferences, objectIdAsMap);
    }

    @Procedure("apoc.mongodb.get.byObjectId")
    @Description("apoc.mongodb.get.byObjectId(hostOrKey, db, collection, objectIdValue, config(default:{})) - get the document by Object id value")
    public Stream<MapResult> byObjectId(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection, @Name("objectIdValue") String objectIdValue, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        MongoDbConfig conf = new MongoDbConfig(config);

        return executeMongoQuery(hostOrKey, db, collection, conf.isCompatibleValues(), conf.isExtractReferences(), conf.isObjectIdAsMap(),
                coll -> {
                    Map<String, Object> result = coll.first(Map.of(conf.getIdFieldName(), new ObjectId(objectIdValue)));
                    return result == null || result.isEmpty() ? Stream.empty() : Stream.of(new MapResult(result));
                },
                e -> log.error("apoc.mongodb.get.byObjectId - hostOrKey = [" + hostOrKey + "], db = [" + db + "], collection = [" + collection + "], objectIdValue = [" + objectIdValue + "]",e));
    }

    private String getMongoDBUrl(String hostOrKey) {
        return new UrlResolver("mongodb", "localhost", 27017).getUrl("mongodb", hostOrKey);
    }

    private  <T> Stream<T> executeMongoQuery(String hostOrKey, String db, String collection, boolean compatibleValues,
                                            boolean extractReferences, boolean objectIdAsMap, Function<Coll, Stream<T>> execute, Consumer<Exception> onError) {
        Coll coll = null;
        try {
            coll = getMongoColl(() -> getColl(hostOrKey, db, collection, compatibleValues, extractReferences, objectIdAsMap));
            return execute.apply(coll).onClose(coll::safeClose);
        } catch (Exception e) {
            if (coll != null) {
                coll.safeClose();
            }
            onError.accept(e);
            throw new RuntimeException(e);
        }
    }
}
