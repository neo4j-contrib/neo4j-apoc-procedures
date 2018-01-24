package apoc.mongodb;

import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.util.MissingDependencyException;
import apoc.util.UrlResolver;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
public class MongoDB {

    @Procedure
    @Description("apoc.mongodb.get(host-or-port,db-or-null,collection-or-null,query-or-null,[compatibleValues=true|false],skip-or-null,limit-or-null) yield value - perform a find operation on mongodb collection")
    public Stream<MapResult> get(@Name("host") String hostOrKey,
                                 @Name("db") String db,
                                 @Name("collection") String collection,
                                 @Name("query") Map<String, Object> query,
                                 @Name(value = "compatibleValues", defaultValue = "false") boolean compatibleValues, @Name(value = "skip", defaultValue = "0") Long skip,
                                 @Name(value = "limit", defaultValue = "0") Long limit) {
        return getMongoColl(hostOrKey, db, collection, compatibleValues).all(query, skip, limit).map(MapResult::new);
    }

    @Procedure
    @Description("apoc.mongodb.count(host-or-port,db-or-null,collection-or-null,query-or-null) yield value - perform a find operation on mongodb collection")
    public Stream<LongResult> count(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection, @Name("query") Map<String, Object> query) {
        long count = getMongoColl(hostOrKey, db, collection, false).count(query);
        return Stream.of(new LongResult(count));
    }

    private Coll getColl(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection, boolean compatibleValues) {
        String url = getMongoDBUrl(hostOrKey);
        return Coll.Factory.create(url, db, collection, compatibleValues);
    }

    @Procedure
    @Description("apoc.mongodb.first(host-or-port,db-or-null,collection-or-null,query-or-null,[compatibleValues=true|false]) yield value - perform a first operation on mongodb collection")
    public Stream<MapResult> first(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection, @Name("query") Map<String, Object> query, @Name(value = "compatibleValues", defaultValue = "false") boolean compatibleValues) {
        Map<String, Object> result = getMongoColl(hostOrKey, db, collection, compatibleValues).first(query);
        return Stream.of(new MapResult(result));
    }

    @Procedure
    @Description("apoc.mongodb.find(host-or-port,db-or-null,collection-or-null,query-or-null,projection-or-null,sort-or-null,pagination,[compatibleValues=true|false],skip-or-null,limit-or-null) yield value - perform a find,project,sort operation on mongodb collection")
    public Stream<MapResult> find(@Name("host") String hostOrKey,
                                  @Name("db") String db,
                                  @Name("collection") String collection,
                                  @Name("query") Map<String, Object> query,
                                  @Name("project") Map<String, Object> project,
                                  @Name("sort") Map<String, Object> sort,
                                  @Name(value = "compatibleValues", defaultValue = "false") boolean compatibleValues,
                                  @Name(value = "skip", defaultValue = "0") Long skip,
                                  @Name(value = "limit", defaultValue = "0") Long limit) {
        return getMongoColl(hostOrKey, db, collection, compatibleValues).find(query, project, sort, skip, limit).map(MapResult::new);
    }

    @Procedure
    @Description("apoc.mongodb.insert(host-or-port,db-or-null,collection-or-null,list-of-maps) - inserts the given documents into the mongodb collection")
    public void insert(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection, @Name("documents") List<Map<String, Object>> documents) {
        getMongoColl(hostOrKey, db, collection, false).insert(documents);
    }

    @Procedure
    @Description("apoc.mongodb.delete(host-or-port,db-or-null,collection-or-null,list-of-maps) - inserts the given documents into the mongodb collection")
    public Stream<LongResult> delete(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection, @Name("query") Map<String, Object> query) {
        return Stream.of(new LongResult(getMongoColl(hostOrKey, db, collection, false).delete(query)));
    }

    @Procedure
    @Description("apoc.mongodb.update(host-or-port,db-or-null,collection-or-null,list-of-maps) - inserts the given documents into the mongodb collection")
    public Stream<LongResult> update(@Name("host") String hostOrKey, @Name("db") String db, @Name("collection") String collection, @Name("query") Map<String, Object> query, @Name("update") Map<String, Object> update) {
        return Stream.of(new LongResult(getMongoColl(hostOrKey, db, collection, false).update(query, update)));
    }

    private String getMongoDBUrl(String hostOrKey) {
        return new UrlResolver("mongodb", "localhost", 27017).getUrl("mongodb", hostOrKey);
    }

    private Coll getMongoColl(String hostOrKey, String db, String collection, boolean compatibleValues) {
        Coll coll = null;
        try {
            coll = getColl(hostOrKey, db, collection, compatibleValues);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException("Cannot find the jar into the plugins folder. \n" +
                    "Please put these jar in the plugins folder :\n\n" +
                    "bson-x.y.z.jar\n" +
                    "\n" +
                    "mongo-java-driver-x.y.z.jar\n" +
                    "\n" +
                    "mongodb-driver-x.y.z.jar\n" +
                    "\n" +
                    "mongodb-driver-core-x.y.z.jar\n" +
                    "\n" +
                    "jackson-annotations-x.y.z.jar\n\njackson-core-x.y.z.jar\n\njackson-databind-x.y.z.jar\n\nSee the documentation: https://neo4j-contrib.github.io/neo4j-apoc-procedures/#_interacting_with_mongodb");
        }
        return coll;
    }

    interface Coll extends Closeable {
        Map<String, Object> first(Map<String, Object> params);

        Stream<Map<String, Object>> all(Map<String, Object> query, Long skip, Long limit);

        long count(Map<String, Object> query);

        Stream<Map<String, Object>> find(Map<String, Object> query, Map<String, Object> project, Map<String, Object> sort, Long skip, Long limit);

        void insert(List<Map<String, Object>> docs);

        long update(Map<String, Object> query, Map<String, Object> update);

        long delete(Map<String, Object> query);

        class Factory {
            public static Coll create(String url, String db, String coll, boolean compatibleValues) {
                try {
                    System.out.println(String.format("%s %s %s %s", url, db, coll, compatibleValues));
                    return (Coll) Class.forName("apoc.mongodb.MongoDBColl").getConstructor(String.class, String.class, String.class, Boolean.class).newInstance(url, db, coll, compatibleValues);
                } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException | ClassNotFoundException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Could not create MongoDBClientWrapper instance", e);
                }
            }
        }
    }
}
