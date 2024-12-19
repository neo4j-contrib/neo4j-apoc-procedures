package apoc.vectordb;

import static apoc.ml.Prompt.API_KEY_CONF;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.ExtendedTestUtil.testRetryCallEventually;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static apoc.util.UtilsExtendedTest.checkEnvVar;
import static apoc.vectordb.VectorDbHandler.Type.PINECONE;
import static apoc.vectordb.VectorDbTestUtil.EntityType.FALSE;
import static apoc.vectordb.VectorDbTestUtil.EntityType.NODE;
import static apoc.vectordb.VectorDbTestUtil.EntityType.REL;
import static apoc.vectordb.VectorDbTestUtil.assertBerlinResult;
import static apoc.vectordb.VectorDbTestUtil.assertLondonResult;
import static apoc.vectordb.VectorDbTestUtil.assertNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertReadOnlyProcWithMappingResults;
import static apoc.vectordb.VectorDbTestUtil.assertRelsCreated;
import static apoc.vectordb.VectorDbTestUtil.dropAndDeleteAll;
import static apoc.vectordb.VectorDbTestUtil.ragSetup;
import static apoc.vectordb.VectorEmbeddingConfig.ALL_RESULTS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorMappingConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import apoc.ml.Prompt;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class PineconeTest {
    private static String API_KEY;
    private static String HOST;

    private static final String collName = UUID.randomUUID().toString();

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    private static Map<String, Object> ADMIN_AUTHORIZATION;
    private static Map<String, Object> ADMIN_HEADER_CONF;

    @BeforeClass
    public static void setUp() {
        API_KEY = checkEnvVar("PINECONE_KEY");
        HOST = checkEnvVar("PINECONE_HOST");

        databaseManagementService =
                new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath()).build();
        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(SYSTEM_DATABASE_NAME);

        TestUtil.registerProcedure(db, VectorDb.class, Pinecone.class, Prompt.class);

        ADMIN_AUTHORIZATION = map("Api-Key", API_KEY);
        ADMIN_HEADER_CONF = map(HEADERS_KEY, ADMIN_AUTHORIZATION);

        testRetryCallEventually(
                db,
                "CALL apoc.vectordb.pinecone.createCollection($host, $coll, 'cosine', 4, $conf)",
                map(
                        "host",
                        HOST,
                        "coll",
                        collName,
                        "conf",
                        map(
                                HEADERS_KEY,
                                ADMIN_AUTHORIZATION,
                                "body",
                                map("spec", map("serverless", map("cloud", "aws", "region", "us-east-1"))))),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals(map("ready", false, "state", "Initializing"), value.get("status"));
                    HOST = "https://" + value.get("host");
                },
                5L);

        // the upsert takes a while
        Util.sleep(5000);

        testResult(
                db,
                "CALL apoc.vectordb.pinecone.upsert($host, $coll,\n" + "[\n"
                        + "    {id: '1', vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: \"Berlin\", foo: \"one\"}},\n"
                        + "    {id: '2', vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: \"London\", foo: \"two\"}}\n"
                        + "],\n"
                        + "$conf)",
                map("host", HOST, "coll", collName, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map<String, Object> row = r.next();
                    Map value = (Map) row.get("value");
                    assertEquals(2L, value.get("upsertedCount"));
                });

        // the upsert takes a while
        Util.sleep(20000);
    }

    @AfterClass
    public static void tearDown() {
        if (API_KEY == null || HOST == null) {
            return;
        }

        Util.sleep(2000);

        testCallEmpty(
                db,
                "CALL apoc.vectordb.pinecone.deleteCollection($host, $coll, $conf)",
                map("host", "", "coll", collName, "conf", ADMIN_HEADER_CONF));

        databaseManagementService.shutdown();
    }

    @Before
    public void before() {
        dropAndDeleteAll(db);
    }

    @Test
    public void getInfo() {
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.info($host, $coll, $conf) ",
                map(
                        "host",
                        null,
                        "coll",
                        collName,
                        "conf",
                        map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    Map value = (Map) row.get("value");
                    assertEquals(collName, value.get("name"));
                });
    }

    @Test
    public void getInfoNotExistentCollection() {
        String wrongCollection = "wrong_collection";
        assertFails(
                db,
                "CALL apoc.vectordb.pinecone.info($host, $coll, $conf)",
                map(
                        "host",
                        null,
                        "coll",
                        wrongCollection,
                        "conf",
                        map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                "java.io.FileNotFoundException: https://api.pinecone.io/indexes/" + wrongCollection);
    }

    @Test
    public void getVectors() {
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.get($host, $coll, ['1', '2'], $conf) "
                        + "YIELD vector, id, metadata, node RETURN * ORDER BY id",
                map(
                        "host",
                        HOST,
                        "coll",
                        collName,
                        "conf",
                        map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, FALSE);
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, FALSE);
                    assertNotNull(row.get("vector"));

                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void getVectorsWithoutVectorResult() {
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.get($host, $coll, ['1'], $conf) ",
                map("host", HOST, "coll", collName, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
                    assertNull(row.get("vector"));
                    assertNull(row.get("id"));

                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void deleteVector() {
        testCall(
                db,
                "CALL apoc.vectordb.pinecone.upsert($host, $coll,\n" + "[\n"
                        + "    {id: '3', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: \"baz\"}},\n"
                        + "    {id: '4', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: \"baz\"}}\n"
                        + "],\n"
                        + "$conf)",
                map("host", HOST, "coll", collName, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals(2L, value.get("upsertedCount"));
                });

        // the upsert takes a while
        Util.sleep(10000);

        testCall(
                db,
                "CALL apoc.vectordb.pinecone.delete($host, $coll, ['3', '4'], $conf) ",
                map("host", HOST, "coll", collName, "conf", ADMIN_HEADER_CONF),
                r -> {
                    assertEquals(Map.of(), r.get("value"));
                });
    }

    @Test
    public void queryVectors() {
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.query($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map(
                        "host",
                        HOST,
                        "coll",
                        collName,
                        "conf",
                        map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, FALSE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, FALSE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });
    }

    @Test
    public void queryVectorsWithoutVectorResult() {
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.queryAndUpdate($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "coll", collName, "conf", map(HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
                    assertNotNull(row.get("score"));
                    assertNull(row.get("vector"));
                    assertNull(row.get("id"));

                    row = r.next();
                    assertEquals(Map.of("city", "London", "foo", "two"), row.get("metadata"));
                    assertNotNull(row.get("score"));
                    assertNull(row.get("vector"));
                    assertNull(row.get("id"));
                });
    }

    @Test
    public void queryVectorsWithYield() {
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.query($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf) YIELD metadata, id",
                map(
                        "host",
                        HOST,
                        "coll",
                        collName,
                        "conf",
                        map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    assertBerlinResult(r.next(), FALSE);
                    assertLondonResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithFilter() {
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.query($host, $coll, [0.2, 0.1, 0.9, 0.7],\n"
                        + "{ city: { `$eq`: \"London\" } },\n"
                        + "5, $conf) YIELD metadata, id",
                map(
                        "host",
                        HOST,
                        "coll",
                        collName,
                        "conf",
                        map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    assertLondonResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithLimit() {
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.query($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 1, $conf) YIELD metadata, id",
                map(
                        "host",
                        HOST,
                        "coll",
                        collName,
                        "conf",
                        map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    assertBerlinResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithCreateNode() {
        Map<String, Object> conf = map(
                ALL_RESULTS_KEY,
                true,
                HEADERS_KEY,
                ADMIN_AUTHORIZATION,
                MAPPING_KEY,
                map(EMBEDDING_KEY, "vect", NODE_LABEL, "Test", ENTITY_KEY, "myId", METADATA_KEY, "foo"));
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.queryAndUpdate($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "coll", collName, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);

        testResult(
                db,
                "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                VectorDbTestUtil::vectorEntityAssertions);

        testResult(
                db,
                "CALL apoc.vectordb.pinecone.queryAndUpdate($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "coll", collName, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }

    @Test
    public void queryVectorsWithCreateNodeUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = map(
                ALL_RESULTS_KEY,
                true,
                HEADERS_KEY,
                ADMIN_AUTHORIZATION,
                MAPPING_KEY,
                map(EMBEDDING_KEY, "vect", NODE_LABEL, "Test", ENTITY_KEY, "myId", METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.pinecone.queryAndUpdate($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "coll", collName, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }

    @Test
    public void getVectorsWithCreateNodeUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = MapUtil.map(
                ALL_RESULTS_KEY,
                true,
                HEADERS_KEY,
                ADMIN_AUTHORIZATION,
                MAPPING_KEY,
                MapUtil.map(EMBEDDING_KEY, "vect", NODE_LABEL, "Test", ENTITY_KEY, "myId", METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.pinecone.getAndUpdate($host, 'TestCollection', [1, 2], $conf) "
                        + "YIELD vector, id, metadata, node RETURN * ORDER BY id",
                Util.map("host", HOST, "coll", collName, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, NODE);
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, NODE);
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }

    @Test
    public void getReadOnlyVectorsWithMapping() {
        db.executeTransactionally("CREATE (:Test {readID: 'one'}), (:Test {readID: 'two'})");

        Map<String, Object> conf = map(
                ALL_RESULTS_KEY,
                true,
                HEADERS_KEY,
                ADMIN_AUTHORIZATION,
                MAPPING_KEY,
                map(
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.pinecone.get($host, 'TestCollection', [1, 2], $conf) "
                        + "YIELD vector, id, metadata, node RETURN * ORDER BY id",
                Util.map("host", HOST, "coll", collName, "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "node"));
    }

    @Test
    public void queryVectorsWithCreateRel() {

        db.executeTransactionally(
                "CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");

        Map<String, Object> conf = map(
                ALL_RESULTS_KEY,
                true,
                HEADERS_KEY,
                ADMIN_AUTHORIZATION,
                MAPPING_KEY,
                map(EMBEDDING_KEY, "vect", REL_TYPE, "TEST", ENTITY_KEY, "myId", METADATA_KEY, "foo"));
        testResult(
                db,
                "CALL apoc.vectordb.pinecone.queryAndUpdate($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "coll", collName, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, REL);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, REL);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertRelsCreated(db);
    }

    @Test
    public void queryReadOnlyVectorsWithMapping() {
        db.executeTransactionally(
                "CREATE (:Start)-[:TEST {readID: 'one'}]->(:End), (:Start)-[:TEST {readID: 'two'}]->(:End)");

        Map<String, Object> conf = map(
                ALL_RESULTS_KEY,
                true,
                HEADERS_KEY,
                ADMIN_AUTHORIZATION,
                MAPPING_KEY,
                map(
                        REL_TYPE, "TEST",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.pinecone.query($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "coll", collName, "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "rel"));
    }

    @Test
    public void queryVectorsWithSystemDbStorage() {
        String keyConfig = "pinecone-config-foo";
        Map<String, Object> mapping =
                map(EMBEDDING_KEY, "vect", NODE_LABEL, "Test", ENTITY_KEY, "myId", METADATA_KEY, "foo");

        sysDb.executeTransactionally(
                "CALL apoc.vectordb.configure($vectorName, $keyConfig, $databaseName, $conf)",
                map(
                        "vectorName",
                        PINECONE.toString(),
                        "keyConfig",
                        keyConfig,
                        "databaseName",
                        DEFAULT_DATABASE_NAME,
                        "conf",
                        map(
                                "host", HOST,
                                "credentials", API_KEY,
                                "mapping", mapping)));

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        testResult(
                db,
                "CALL apoc.vectordb.pinecone.queryAndUpdate($host, $coll, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", keyConfig, "coll", collName, "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }

    @Test
    public void queryVectorsWithRag() {
        String openAIKey = ragSetup(db);

        Map<String, Object> conf = map(
                ALL_RESULTS_KEY,
                true,
                HEADERS_KEY,
                ADMIN_AUTHORIZATION,
                MAPPING_KEY,
                map(NODE_LABEL, "Rag", ENTITY_KEY, "readID", METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.pinecone.getAndUpdate($host, $collection, ['1', '2'], $conf) YIELD node, metadata, id, vector\n"
                        + "WITH collect(node) as paths\n"
                        + "CALL apoc.ml.rag(paths, $attributes, \"Which city has foo equals to one?\", $confPrompt) YIELD value\n"
                        + "RETURN value",
                map(
                        "host", HOST,
                        "conf", conf,
                        "collection", collName,
                        "confPrompt", map(API_KEY_CONF, openAIKey),
                        "attributes", List.of("city", "foo")),
                VectorDbTestUtil::assertRagWithVectors);
    }
}
