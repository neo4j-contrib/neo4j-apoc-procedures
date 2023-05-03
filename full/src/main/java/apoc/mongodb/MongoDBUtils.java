/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.mongodb;

import apoc.util.JsonUtil;
import apoc.util.MissingDependencyException;
import apoc.util.Util;
import apoc.version.Version;
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

    protected static Document getDocument(Object query) {
        if (query == null) {
            return new Document();
        }
        return Document.parse(query instanceof String
                ? (String) query
                : JsonUtil.writeValueAsString(query));
    }
}
