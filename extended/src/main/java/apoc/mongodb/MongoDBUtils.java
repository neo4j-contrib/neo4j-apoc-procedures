package apoc.mongodb;

import apoc.util.JsonUtil;
import apoc.util.MissingDependencyException;
import apoc.util.Util;
import org.bson.Document;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class MongoDBUtils {
    interface Coll extends Closeable {
        Map<String, Object> first(Map<String, Object> params);

        Stream<Map<String, Object>> all(Map<String, Object> query, Long skip, Long limit);

        long count(Map<String, Object> query);
        long count(Document query);

        Stream<Map<String, Object>> aggregate(List<Document> pipeline);

        Stream<Map<String, Object>> find(Map<String, Object> query, Map<String, Object> project, Map<String, Object> sort, Long skip, Long limit);
        Stream<Map<String, Object>> find(Document query, Document project, Document sort, int skip, int limit);

        void insert(List<Map<String, Object>> docs);
        void insertDocs(List<Document> docs);

        long update(Map<String, Object> query, Map<String, Object> update);
        long update(Document query, Document update);

        long delete(Map<String, Object> query);
        long delete(Document query);

        default void safeClose() {
            Util.close(this);
        }

        class Factory {
            public static Coll create(String url, String db, String coll, boolean compatibleValues,
                                      boolean extractReferences,
                                      boolean objectIdAsMap) {
                try {
                    return new MongoDBColl(url, db, coll, compatibleValues, extractReferences, objectIdAsMap);
                } catch (Exception e) {
                    throw new RuntimeException("Could not create MongoDBColl instance", e);
                }
            }

            public static Coll create(String url, MongoDbConfig conf) {
                try {
                    return new MongoDBColl(url, conf);
                } catch (Exception e) {
                    throw new RuntimeException("Could not create MongoDBColl instance", e);
                }
            }
        }
    }

    protected static MongoDBUtils.Coll getMongoColl(Supplier<Coll> action) {
        Coll coll = null;
        try {
            coll = action.get();
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(String.format("Cannot find the jar into the plugins folder. \n" +
                    "Please put these jar in the plugins folder :\n\n" +
                    "bson-x.y.z.jar\n" +
                    "\n" +
                    "mongo-java-driver-x.y.z.jar\n" +
                    "\n" +
                    "mongodb-driver-x.y.z.jar\n" +
                    "\n" +
                    "mongodb-driver-core-x.y.z.jar\n" +
                    "\n" +
                    "jackson-annotations-x.y.z.jar\n\njackson-core-x.y.z.jar\n\njackson-databind-x.y.z.jar\n\nSee the documentation: https://neo4j.com/labs/apoc/4.4/database-integration/mongodb/"));
        } catch (Exception e) {
            throw new RuntimeException("Error during connection", e);
        }
        return coll;
    }

    protected static Document getDocument(Object query) {
        if (query == null) {
            return new Document();
        }
        return Document.parse(query instanceof String
                ? (String) query
                : JsonUtil.writeValueAsString(query));
    }
}
