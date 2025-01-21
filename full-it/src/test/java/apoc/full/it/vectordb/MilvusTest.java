package apoc.full.it.vectordb;

import static apoc.ml.Prompt.API_KEY_CONF;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.vectordb.VectorDbHandler.Type.MILVUS;
import static apoc.vectordb.VectorDbTestUtil.EntityType.FALSE;
import static apoc.vectordb.VectorDbTestUtil.EntityType.NODE;
import static apoc.vectordb.VectorDbTestUtil.EntityType.REL;
import static apoc.vectordb.VectorDbTestUtil.assertBerlinResult;
import static apoc.vectordb.VectorDbTestUtil.assertLondonResult;
import static apoc.vectordb.VectorDbTestUtil.assertNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertReadOnlyProcWithMappingResults;
import static apoc.vectordb.VectorDbTestUtil.assertRelsCreated;
import static apoc.vectordb.VectorDbTestUtil.dropAndDeleteAll;
import static apoc.vectordb.VectorDbTestUtil.getAuthHeader;
import static apoc.vectordb.VectorDbTestUtil.ragSetup;
import static apoc.vectordb.VectorEmbeddingConfig.ALL_RESULTS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.FIELDS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorMappingConfig.EMBEDDING_KEY;
import static apoc.vectordb.VectorMappingConfig.ENTITY_KEY;
import static apoc.vectordb.VectorMappingConfig.METADATA_KEY;
import static apoc.vectordb.VectorMappingConfig.MODE_KEY;
import static apoc.vectordb.VectorMappingConfig.MappingMode;
import static apoc.vectordb.VectorMappingConfig.NODE_LABEL;
import static apoc.vectordb.VectorMappingConfig.NO_FIELDS_ERROR_MSG;
import static apoc.vectordb.VectorMappingConfig.REL_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import apoc.ml.Prompt;
import apoc.util.ExtendedTestUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.vectordb.Milvus;
import apoc.vectordb.VectorDb;
import apoc.vectordb.VectorDbTestUtil;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.testcontainers.milvus.MilvusContainer;

public class MilvusTest {
    private static final List<String> FIELDS = List.of("city", "foo");
    private static final MilvusContainer MILVUS_CONTAINER = new MilvusContainer("milvusdb/milvus:v2.4.0");
    private static final String READONLY_KEY = "my_readonly_api_key";
    private static final Map<String, String> READONLY_AUTHORIZATION = getAuthHeader(READONLY_KEY);

    private static String HOST;

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void setUp() throws Exception {
        databaseManagementService =
                new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath()).build();
        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(SYSTEM_DATABASE_NAME);

        MILVUS_CONTAINER.start();

        HOST = MILVUS_CONTAINER.getEndpoint();
        TestUtil.registerProcedure(db, Milvus.class, VectorDb.class, Prompt.class);

        testCall(
                db,
                "CALL apoc.vectordb.milvus.createCollection($host, 'test_collection', 'COSINE', 4)",
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals(200L, value.get("code"));
                });

        testCall(
                db,
                "CALL apoc.vectordb.milvus.upsert($host, 'test_collection',\n" + "[\n"
                        + "    {id: 1, vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: \"Berlin\", foo: \"one\"}},\n"
                        + "    {id: 2, vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: \"London\", foo: \"two\"}}\n"
                        + "])",
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals(200L, value.get("code"));
                });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCall(db, "CALL apoc.vectordb.milvus.deleteCollection($host, 'test_collection')", map("host", HOST), r -> {
            Map value = (Map) r.get("value");
            assertEquals(200L, value.get("code"));
        });

        databaseManagementService.shutdown();
        MILVUS_CONTAINER.stop();
    }

    @Before
    public void before() {
        dropAndDeleteAll(db);
    }

    @Test
    public void getInfo() {
        testResult(
                db,
                "CALL apoc.vectordb.milvus.info($host, 'test_collection', '', $conf) ",
                map("host", HOST, "conf", map(FIELDS_KEY, FIELDS)),
                r -> {
                    Map<String, Object> row = r.next();
                    Map value = (Map) row.get("value");
                    assertEquals(200L, value.get("code"));
                });
    }

    @Test
    public void getInfoNotExistentCollection() {
        testResult(
                db,
                "CALL apoc.vectordb.milvus.info($host, 'wrong_collection', '', $conf) ",
                map("host", HOST, "conf", map(FIELDS_KEY, FIELDS)),
                r -> {
                    Map<String, Object> row = r.next();
                    Map value = (Map) row.get("value");
                    assertEquals(100L, value.get("code"));
                });
    }

    @Test
    public void getVectorsWithoutVectorResult() {
        testResult(
                db,
                "CALL apoc.vectordb.milvus.get($host, 'test_collection', [1], $conf) ",
                map("host", HOST, "conf", map(FIELDS_KEY, FIELDS)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
                    assertNull(row.get("vector"));
                    assertNull(row.get("id"));
                });
    }

    @Test
    public void deleteVector() {
        testCall(
                db,
                "CALL apoc.vectordb.milvus.upsert($host, 'test_collection',\n" + "[\n"
                        + "    {id: 3, vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: \"baz\"}},\n"
                        + "    {id: 4, vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: \"baz\"}}\n"
                        + "])",
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals(200L, value.get("code"));
                });

        testCall(db, "CALL apoc.vectordb.milvus.delete($host, 'test_collection', [3, 4]) ", map("host", HOST), r -> {
            Map value = (Map) r.get("value");
            assertEquals(200L, value.get("code"));
        });

        Util.sleep(2000);
    }

    @Test
    public void queryVectors() {
        testResult(
                db,
                "CALL apoc.vectordb.milvus.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", HOST, "conf", map(FIELDS_KEY, FIELDS, ALL_RESULTS_KEY, true)),
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
                "CALL apoc.vectordb.milvus.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", HOST, "conf", map(FIELDS_KEY, FIELDS)),
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
                "CALL apoc.vectordb.milvus.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) YIELD metadata, id",
                map("host", HOST, "conf", map(FIELDS_KEY, FIELDS, ALL_RESULTS_KEY, true)),
                r -> {
                    assertBerlinResult(r.next(), FALSE);
                    assertLondonResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithFilter() {
        testResult(
                db,
                "CALL apoc.vectordb.milvus.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7],\n"
                        + "'city == \"London\"',\n"
                        + "5, $conf) YIELD metadata, id",
                map("host", HOST, "conf", map(FIELDS_KEY, FIELDS, ALL_RESULTS_KEY, true)),
                r -> {
                    assertLondonResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithLimit() {
        testResult(
                db,
                "CALL apoc.vectordb.milvus.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 1, $conf) YIELD metadata, id",
                map("host", HOST, "conf", map(FIELDS_KEY, FIELDS, ALL_RESULTS_KEY, true)),
                r -> {
                    assertBerlinResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithCreateNode() {

        Map<String, Object> conf = map(
                FIELDS_KEY,
                FIELDS,
                ALL_RESULTS_KEY,
                true,
                MAPPING_KEY,
                map(
                        EMBEDDING_KEY,
                        "vect",
                        NODE_LABEL,
                        "Test",
                        ENTITY_KEY,
                        "myId",
                        METADATA_KEY,
                        "foo",
                        MODE_KEY,
                        MappingMode.CREATE_IF_MISSING.toString()));
        testResult(
                db,
                "CALL apoc.vectordb.milvus.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", HOST, "conf", conf),
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
                "CALL apoc.vectordb.milvus.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", HOST, "conf", conf),
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

        Map<String, Object> conf = map(
                ALL_RESULTS_KEY,
                true,
                FIELDS_KEY,
                FIELDS,
                MAPPING_KEY,
                map(EMBEDDING_KEY, "vect", NODE_LABEL, "Test", ENTITY_KEY, "myId", METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.milvus.getAndUpdate($host, 'test_collection', [1, 2], $conf) "
                        + "YIELD vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
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
                FIELDS_KEY,
                FIELDS,
                MAPPING_KEY,
                map(EMBEDDING_KEY, "vect", NODE_LABEL, "Test", ENTITY_KEY, "readID", METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.milvus.get($host, 'test_collection', [1, 2], $conf) "
                        + "YIELD vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "node"));
    }

    @Test
    public void queryVectorsWithCreateNodeUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = map(
                FIELDS_KEY,
                FIELDS,
                ALL_RESULTS_KEY,
                true,
                MAPPING_KEY,
                map(EMBEDDING_KEY, "vect", NODE_LABEL, "Test", ENTITY_KEY, "myId", METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.milvus.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", HOST, "conf", conf),
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
    public void queryVectorsWithCreateRel() {

        db.executeTransactionally(
                "CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");

        Map<String, Object> conf = map(
                FIELDS_KEY,
                FIELDS,
                ALL_RESULTS_KEY,
                true,
                MAPPING_KEY,
                map(EMBEDDING_KEY, "vect", REL_TYPE, "TEST", ENTITY_KEY, "myId", METADATA_KEY, "foo"));
        testResult(
                db,
                "CALL apoc.vectordb.milvus.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", HOST, "conf", conf),
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
                FIELDS_KEY,
                FIELDS,
                MAPPING_KEY,
                map(
                        REL_TYPE, "TEST",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.milvus.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", HOST, "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "rel"));
    }

    @Test
    public void queryVectorsWithSystemDbStorage() {
        String keyConfig = "milvus-config-foo";
        Map<String, Object> mapping =
                map(EMBEDDING_KEY, "vect", NODE_LABEL, "Test", ENTITY_KEY, "myId", METADATA_KEY, "foo");

        sysDb.executeTransactionally(
                "CALL apoc.vectordb.configure($vectorName, $keyConfig, $databaseName, $conf)",
                map(
                        "vectorName",
                        MILVUS.toString(),
                        "keyConfig",
                        keyConfig,
                        "databaseName",
                        DEFAULT_DATABASE_NAME,
                        "conf",
                        map("host", HOST + "/v2/vectordb", "mapping", mapping)));

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        testResult(
                db,
                "CALL apoc.vectordb.milvus.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", keyConfig, "conf", map(FIELDS_KEY, FIELDS, ALL_RESULTS_KEY, true)),
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
                FIELDS_KEY,
                FIELDS,
                ALL_RESULTS_KEY,
                true,
                HEADERS_KEY,
                READONLY_AUTHORIZATION,
                MAPPING_KEY,
                map(NODE_LABEL, "Rag", ENTITY_KEY, "readID", METADATA_KEY, "foo"));

        testResult(
                db,
                "CALL apoc.vectordb.milvus.getAndUpdate($host, 'test_collection', [1, 2], $conf) YIELD node, metadata, id, vector\n"
                        + "WITH collect(node) as paths\n"
                        + "CALL apoc.ml.rag(paths, $attributes, \"Which city has foo equals to one?\", $confPrompt) YIELD value\n"
                        + "RETURN value",
                map(
                        "host",
                        HOST,
                        "conf",
                        conf,
                        "confPrompt",
                        map(API_KEY_CONF, openAIKey),
                        "attributes",
                        List.of("city", "foo")),
                VectorDbTestUtil::assertRagWithVectors);
    }

    @Test
    public void queryVectorsWithMetadataKeyNoFields() {
        Map<String, Object> conf = map(
                ALL_RESULTS_KEY,
                true,
                MAPPING_KEY,
                map(EMBEDDING_KEY, "vect", REL_TYPE, "TEST", ENTITY_KEY, "readID", METADATA_KEY, "foo"));
        testResult(
                db,
                "CALL apoc.vectordb.milvus.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", HOST, "conf", conf),
                VectorDbTestUtil::assertMetadataFooResult);
    }

    @Test
    public void queryVectorsWithNoMetadataKeyNoFields() {
        Map<String, Object> params = map(
                "host",
                HOST,
                "conf",
                Map.of(
                        ALL_RESULTS_KEY,
                        true,
                        MAPPING_KEY,
                        map(EMBEDDING_KEY, "vect", REL_TYPE, "TEST", ENTITY_KEY, "readID")));
        String query =
                "CALL apoc.vectordb.milvus.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)";
        ExtendedTestUtil.assertFails(db, query, params, NO_FIELDS_ERROR_MSG);
    }

    @Test
    public void queryAndUpdateMetadataKeyWithoutFieldsTest() {
        Map<String, Object> conf = map(
                ALL_RESULTS_KEY,
                true,
                MAPPING_KEY,
                map(EMBEDDING_KEY, "vect", REL_TYPE, "TEST", ENTITY_KEY, "readID", METADATA_KEY, "foo"));

        String query =
                "CALL apoc.vectordb.milvus.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)";

        testResult(db, query, map("host", HOST, "conf", conf), VectorDbTestUtil::assertMetadataFooResult);
    }

    @Test
    public void queryAndUpdateWithNoMetadataKeyNoFields() {
        Map<String, Object> conf = map(
                ALL_RESULTS_KEY, true, MAPPING_KEY, map(EMBEDDING_KEY, "vect", REL_TYPE, "TEST", ENTITY_KEY, "readID"));
        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");
        Map<String, Object> params = Util.map("host", HOST, "conf", conf);

        String query =
                "CALL apoc.vectordb.milvus.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)";

        ExtendedTestUtil.assertFails(db, query, params, NO_FIELDS_ERROR_MSG);
    }
}
