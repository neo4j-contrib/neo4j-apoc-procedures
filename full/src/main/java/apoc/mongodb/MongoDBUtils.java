package apoc.mongodb;

import apoc.util.MissingDependencyException;
import apoc.version.Version;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class MongoDBUtils {
    interface Coll extends Closeable {
        Map<String, Object> first(Map<String, Object> params);
        Map<String, Object> first(Map<String, Object> params, boolean useExtendedJson);

        Stream<Map<String, Object>> all(Map<String, Object> query, Long skip, Long limit);
        Stream<Map<String, Object>> all(Map<String, Object> query, Long skip, Long limit, boolean useExtendedJson);

        long count(Map<String, Object> query);
        long count(Map<String, Object> query, boolean useExtendedJson);

        Stream<Map<String, Object>> find(Map<String, Object> query, Map<String, Object> project, Map<String, Object> sort, Long skip, Long limit);
        Stream<Map<String, Object>> find(Map<String, Object> query, Map<String, Object> project, Map<String, Object> sort, Long skip, Long limit, boolean useExtendedJson);

        void insert(List<Map<String, Object>> docs);
        void insert(List<Map<String, Object>> docs, boolean useExtendedJson);

        long update(Map<String, Object> query, Map<String, Object> update);
        long update(Map<String, Object> query, Map<String, Object> update, boolean useExtendedJson);

        long delete(Map<String, Object> query);
        long delete(Map<String, Object> query, boolean useExtendedJson);

        default void safeClose() {
            try {
                this.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        class Factory {
            public static Coll create(String url, String db, String coll, boolean compatibleValues,
                                      boolean extractReferences,
                                      boolean objectIdAsMap) {
                try {
                    return new MongoDBColl(url, db, coll, compatibleValues, extractReferences, objectIdAsMap);
                } catch (Exception e) {
                    throw new RuntimeException("Could not create MongoDBClientWrapper instance", e);
                }
            }

            public static Coll create(String url, MongoDbConfig conf) {
                try {
                    return new MongoDBColl(url, conf);
                } catch (Exception e) {
                    throw new RuntimeException("Could not create MongoDBClientWrapper instance", e);
                }
            }
        }
    }

    protected static MongoDBUtils.Coll withMongoColl(Supplier<Coll> action) {
        Coll coll = null;
        try {
            coll = action.get();
        } catch (NoClassDefFoundError e) {
            final String version = Version.class.getPackage().getImplementationVersion().substring(0, 3);
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
                    "jackson-annotations-x.y.z.jar\n\njackson-core-x.y.z.jar\n\njackson-databind-x.y.z.jar\n\nSee the documentation: https://neo4j.com/labs/apoc/%s/database-integration/mongodb/", version));
        } catch (Exception e) {
            throw new RuntimeException("Error during connection", e);
        }
        return coll;
    }
}
