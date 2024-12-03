package apoc.mongodb;

import apoc.util.UtilExtended;
import org.bson.Document;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public interface MongoDbCollInterface extends Closeable {
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
        UtilExtended.close(this);
    }

    class Factory {
        public static MongoDbCollInterface create(String url, String db, String coll, boolean compatibleValues,
                                  boolean extractReferences,
                                  boolean objectIdAsMap) {
            try {
                return new MongoDBColl(url, db, coll, compatibleValues, extractReferences, objectIdAsMap);
            } catch (Exception e) {
                throw new RuntimeException("Could not create MongoDBColl instance", e);
            }
        }

        public static MongoDbCollInterface create(String url, MongoDbConfig conf) {
            try {
                return new MongoDBColl(url, conf);
            } catch (Exception e) {
                throw new RuntimeException("Could not create MongoDBColl instance", e);
            }
        }
    }
}
