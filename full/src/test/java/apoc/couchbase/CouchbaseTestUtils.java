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
package apoc.couchbase;

import static apoc.util.TestUtil.isRunningInCI;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.testcontainers.containers.Container;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

public class CouchbaseTestUtils {

    public static final String BUCKET_NAME = "mybucket";

    public static final String USERNAME = "admin";
    public static final String PASSWORD = "secret";

    public static final String COLL_NAME = "another";
    public static final String SECOND_COLL_NAME = "anotherTwo";
    public static final String SECOND_SCOPE = "secondScope";

    private static final String QUERY = "select * from %s where lastName = 'Van Gogh'";

    protected static final String COUCHBASE_CONFIG_KEY = "demo";
    protected static final String BASE_APOC_CONFIG = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY;
    protected static final String BASE_CONFIG_KEY = BASE_APOC_CONFIG + COUCHBASE_CONFIG_KEY + ".";
    protected static String COUCHBASE_HOST;

    protected static CouchbaseContainer couchbase;
    protected static Collection collection;
    protected static String HOST = null;

    public static final JsonObject BIG_JSON = JsonObject.from(
            IntStream.range(0, 99999).boxed().collect(Collectors.toMap(Object::toString, Object::toString)));

    public static final JsonObject VINCENT_VAN_GOGH = JsonObject.create()
            .put("firstName", "Vincent")
            .put("secondName", "Willem")
            .put("lastName", "Van Gogh")
            .put(
                    "notableWorks",
                    JsonArray.from(
                            "Starry Night", "Sunflowers", "Bedroom in Arles", "Portrait of Dr Gachet", "Sorrow"));

    public static boolean fillDB(Cluster cluster) {
        Bucket couchbaseBucket = cluster.bucket(BUCKET_NAME);
        Collection collection = couchbaseBucket.defaultCollection();
        collection.insert("artist:vincent_van_gogh", VINCENT_VAN_GOGH);
        QueryResult queryResult = cluster.query(
                String.format(QUERY, BUCKET_NAME), queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
        final CollectionManager collectionManager = couchbaseBucket.collections();
        collectionManager.createScope(SECOND_SCOPE);
        collectionManager.createCollection(CollectionSpec.create(COLL_NAME, CollectionIdentifier.DEFAULT_COLLECTION));
        collectionManager.createCollection(CollectionSpec.create(SECOND_COLL_NAME, SECOND_SCOPE));

        Collection anotherCollection = couchbaseBucket.collection(COLL_NAME);
        anotherCollection.insert("foo:bar", JsonObject.create().put("alpha", "beta"));

        Collection secondScopeCollection = couchbaseBucket.scope(SECOND_SCOPE).collection(SECOND_COLL_NAME);
        secondScopeCollection.insert(SECOND_SCOPE, JsonObject.create().put("one", "two"));

        return queryResult.rowsAsObject().size() == 1;
    }

    public static String getUrl(CouchbaseContainer couchbaseContainer) {
        return String.format(
                "couchbase://%s:%s@%s:%s",
                USERNAME,
                PASSWORD,
                couchbaseContainer.getContainerIpAddress(),
                couchbaseContainer.getFirstMappedPort());
    }

    @SuppressWarnings("unchecked")
    public static void checkListResult(Map<String, Object> r) {
        assertTrue(r.get("queryResult") instanceof List);
        List<Map<String, Object>> listResult = (List<Map<String, Object>>) r.get("queryResult");
        assertNotNull(listResult);
        assertEquals(1, listResult.size());
        assertTrue(listResult.get(0) instanceof Map);
        Map<String, Object> result = (Map<String, Object>) listResult.get(0);
        checkResult(result);
    }

    @SuppressWarnings("unchecked")
    public static void checkResult(Map<String, Object> result) {
        assertTrue(result.get(BUCKET_NAME) instanceof Map);
        Map<String, Object> content = (Map<String, Object>) result.get(BUCKET_NAME);
        assertTrue(content.get("notableWorks") instanceof List);
        List<String> notableWorks = (List<String>) content.get("notableWorks");

        checkDocumentContent(
                (String) content.get("firstName"),
                (String) content.get("secondName"),
                (String) content.get("lastName"),
                notableWorks);
    }

    public static void checkDocumentContent(
            String firstName, String secondName, String lastName, List<String> notableWorks) {
        assertEquals("Vincent", firstName);
        assertEquals("Willem", secondName);
        assertEquals("Van Gogh", lastName);
        assertEquals(5, notableWorks.size());
        assertTrue(notableWorks.contains("Starry Night"));
        assertTrue(notableWorks.contains("Sunflowers"));
        assertTrue(notableWorks.contains("Bedroom in Arles"));
        assertTrue(notableWorks.contains("Portrait of Dr Gachet"));
        assertTrue(notableWorks.contains("Sorrow"));
    }

    protected static void createCouchbaseContainer() {
        assumeFalse(isRunningInCI());
        // 7.0 support stably multi collections and scopes
        couchbase = new CouchbaseContainer("couchbase/server:7.0.0")
                .withCredentials(USERNAME, PASSWORD)
                .withExposedPorts(8091, 8092, 8093, 8094, 11207, 11210, 11211, 18091, 18092, 18093)
                .withBucket(new BucketDefinition(BUCKET_NAME));
        couchbase.start();
        COUCHBASE_HOST = couchbase.getHost();

        Cluster cluster =
                Cluster.connect(couchbase.getConnectionString(), couchbase.getUsername(), couchbase.getPassword());
        cluster.waitUntilReady(Duration.of(30, ChronoUnit.SECONDS));

        fillDB(cluster);
        HOST = getUrl(couchbase);
        Bucket bucket = cluster.bucket(BUCKET_NAME);
        collection = bucket.defaultCollection();
    }

    protected static int getNumConnections() {
        try {
            final Container.ExecResult execResult = couchbase.execInContainer(
                    "cbstats", COUCHBASE_HOST + ":11210", "-p", PASSWORD, "-u", USERNAME, "-a", "all");
            return Stream.of(execResult.getStdout().split(System.lineSeparator()))
                    .filter(line -> line.contains("curr_connections"))
                    .findFirst()
                    .map(s -> s.split(":")[1])
                    .map(String::trim)
                    .map(Integer::parseInt)
                    .orElse(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
