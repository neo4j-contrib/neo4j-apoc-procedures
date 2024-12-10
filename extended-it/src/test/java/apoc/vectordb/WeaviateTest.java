package apoc.vectordb;

import apoc.ml.Prompt;
import apoc.util.ExtendedTestUtil;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.WeaviateTestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.util.List;
import java.util.Map;

import static apoc.ml.Prompt.API_KEY_CONF;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static apoc.util.WeaviateTestUtil.ADMIN_AUTHORIZATION;
import static apoc.util.WeaviateTestUtil.ADMIN_HEADER_CONF;
import static apoc.util.WeaviateTestUtil.ADMIN_KEY;
import static apoc.util.WeaviateTestUtil.COLLECTION_NAME;
import static apoc.util.WeaviateTestUtil.FIELDS;
import static apoc.util.WeaviateTestUtil.HOST_WEAVIATE;
import static apoc.util.WeaviateTestUtil.ID_1;
import static apoc.util.WeaviateTestUtil.ID_2;
import static apoc.util.WeaviateTestUtil.READONLY_AUTHORIZATION;
import static apoc.util.WeaviateTestUtil.WEAVIATE_CONTAINER;
import static apoc.util.WeaviateTestUtil.WEAVIATE_CREATE_COLLECTION_APOC;
import static apoc.util.WeaviateTestUtil.WEAVIATE_DELETE_COLLECTION_APOC;
import static apoc.util.WeaviateTestUtil.WEAVIATE_DELETE_VECTOR_APOC;
import static apoc.util.WeaviateTestUtil.WEAVIATE_QUERY_APOC;
import static apoc.util.WeaviateTestUtil.WEAVIATE_UPSERT_QUERY;
import static apoc.vectordb.VectorDbHandler.Type.WEAVIATE;
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
import static apoc.vectordb.VectorEmbeddingConfig.FIELDS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorMappingConfig.EMBEDDING_KEY;
import static apoc.vectordb.VectorMappingConfig.ENTITY_KEY;
import static apoc.vectordb.VectorMappingConfig.METADATA_KEY;
import static apoc.vectordb.VectorMappingConfig.MODE_KEY;
import static apoc.vectordb.VectorMappingConfig.MappingMode;
import static apoc.vectordb.VectorMappingConfig.NODE_LABEL;
import static apoc.vectordb.VectorMappingConfig.REL_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;


public class WeaviateTest {

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void setUp() throws Exception {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath())
                .build();
        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(SYSTEM_DATABASE_NAME);
        
        WEAVIATE_CONTAINER.start();
        HOST_WEAVIATE = WEAVIATE_CONTAINER.getHttpHostAddress();

        TestUtil.registerProcedure(db, Weaviate.class, VectorDb.class, Prompt.class);

        testCall(db, WEAVIATE_CREATE_COLLECTION_APOC,
                MapUtil.map("host", HOST_WEAVIATE, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("TestCollection", value.get("class"));
                });

        testResult(db, WEAVIATE_UPSERT_QUERY,
                MapUtil.map("host", HOST_WEAVIATE, "id1", ID_1, "id2", ID_2, "conf", ADMIN_HEADER_CONF),
                r -> {
                    ResourceIterator<Map> values = r.columnAs("value");
                    assertEquals(COLLECTION_NAME, values.next().get("class"));
                    assertEquals(COLLECTION_NAME, values.next().get("class"));
                    assertEquals(COLLECTION_NAME, values.next().get("class"));
                    assertEquals(COLLECTION_NAME, values.next().get("class"));
                    assertFalse(values.hasNext());
                });
        
        // -- delete vector
        testCall(db, WEAVIATE_DELETE_VECTOR_APOC,
                map("host", HOST_WEAVIATE, "conf", ADMIN_HEADER_CONF),
                r -> {
                    List value = (List) r.get("value");
                    assertEquals(List.of("7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308", "7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309"), value);
                });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCallEmpty(db, WEAVIATE_DELETE_COLLECTION_APOC,
                MapUtil.map("host", HOST_WEAVIATE, "collectionName", COLLECTION_NAME, "conf", ADMIN_HEADER_CONF)
        );

        WEAVIATE_CONTAINER.stop();
        databaseManagementService.shutdown();
    }

    @Before
    public void before() {
        dropAndDeleteAll(db);
    }

    @Test
    public void getInfo() {
        testResult(db, "CALL apoc.vectordb.weaviate.info($host, $collectionName, $conf)",
                map("host", HOST_WEAVIATE, "collectionName", COLLECTION_NAME, "conf", map(ALL_RESULTS_KEY, true, HEADERS_KEY, READONLY_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    Map value = (Map) row.get("value");
                    assertEquals(COLLECTION_NAME, value.get("class"));
                });
    }

    @Test
    public void getInfoNotExistentCollection() {
        assertFails(
                db,
                "CALL apoc.vectordb.weaviate.info($host, 'wrong_collection', $conf)",
                map("host", HOST_WEAVIATE, "collectionName", COLLECTION_NAME, "conf", map(ALL_RESULTS_KEY, true, HEADERS_KEY, READONLY_AUTHORIZATION)),
                "java.io.FileNotFoundException"
        );
    }

    @Test
    public void getVectorsWithReadOnlyApiKey() {
        testResult(db, "CALL apoc.vectordb.weaviate.get($host, 'TestCollection', [$id1], $conf)",
                map("host", HOST_WEAVIATE, "id1", ID_1, "conf", map(ALL_RESULTS_KEY, true, HEADERS_KEY, READONLY_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, ID_1, FALSE);
                    assertNotNull(row.get("vector"));
                });
    }
    
    @Test
    public void writeOperationWithReadOnlyUser() {
        try {
            testCall(db, "CALL apoc.vectordb.weaviate.deleteCollection($host, 'TestCollection', $conf)",
                    map("host", HOST_WEAVIATE, 
                            "conf", map(HEADERS_KEY, READONLY_AUTHORIZATION)
                    ),
                    r -> fail()
            );
        } catch (Exception e) {
            assertThat( e.getMessage() ).contains("HTTP response code: 403");
        }
    }
    
    @Test
    public void getVectorsWithoutVectorResult() {
        testResult(db, "CALL apoc.vectordb.weaviate.get($host, 'TestCollection', [$id1], $conf)",
                map("host", HOST_WEAVIATE, "id1", ID_1, "conf", map(HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
                    assertNull(row.get("vector"));
                    assertNull(row.get("id"));
                });
    }

    @Test
    public void queryVectors() {
        testResult(db, WEAVIATE_QUERY_APOC,
                map("host", HOST_WEAVIATE, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                WeaviateTestUtil::queryVectorsAssertions);
    }

    @Test
    public void queryVectorsWithoutVectorResult() {
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST_WEAVIATE, "conf", map( FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
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
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       "YIELD metadata, id RETURN * ORDER BY id",
                map("host", HOST_WEAVIATE, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    assertBerlinResult(r.next(), ID_1, FALSE);
                    assertLondonResult(r.next(), ID_2, FALSE);
                });
    }

    @Test
    public void queryVectorsWithFilter() {
        testResult(db, """
                        CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7],
                        '{operator: Equal, valueString: "London", path: ["city"]}',
                        5, $conf) YIELD metadata, id RETURN * ORDER BY id""",
                map("host", HOST_WEAVIATE, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    assertLondonResult(r.next(), ID_2, FALSE);
                });
    }

    @Test
    public void queryVectorsWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 1, $conf) YIELD metadata, id RETURN * ORDER BY id""",
                map("host", HOST_WEAVIATE, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    assertBerlinResult(r.next(), ID_1, FALSE);
                });
    }

    @Test
    public void queryVectorsWithCreateNode() {

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true, 
                FIELDS_KEY, FIELDS,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                    NODE_LABEL, "Test",
                    ENTITY_KEY, "myId",
                    METADATA_KEY, "foo", 
                    MODE_KEY, MappingMode.CREATE_IF_MISSING.toString()
                )
        );
        testResult(db, "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       "YIELD score, vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST_WEAVIATE, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, ID_1, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, ID_2, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                VectorDbTestUtil::vectorEntityAssertions);

        testResult(db, "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST_WEAVIATE, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, ID_1, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, ID_2, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }

    @Test
    public void queryVectorsWithCreateNodeUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true, 
                FIELDS_KEY, FIELDS,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                NODE_LABEL, "Test",
                ENTITY_KEY, "myId",
                METADATA_KEY, "foo"));
        testResult(db, "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST_WEAVIATE, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, ID_1, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, ID_2, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }
    
    @Test
    public void getVectorsWithCreateNodeUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = MapUtil.map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, MapUtil.map(EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "myId",
                        METADATA_KEY, "foo"));

        testResult(db, "CALL apoc.vectordb.weaviate.getAndUpdate($host, 'TestCollection', [$id1, $id2], $conf)",
                map("host", HOST_WEAVIATE, "id1", ID_1, "id2", ID_2, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, ID_1, NODE);
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row,  ID_2, NODE);
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }


    @Test
    public void getReadOnlyVectorsWithMapping() {
        db.executeTransactionally("CREATE (:Test {readID: 'one'}), (:Test {readID: 'two'})");

        Map<String, Object> conf = MapUtil.map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, READONLY_AUTHORIZATION,
                MAPPING_KEY, MapUtil.map(
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo")
        );

        testResult(db, "CALL apoc.vectordb.weaviate.get($host, 'TestCollection', [$id1, $id2], $conf) " +
                       "YIELD vector, id, metadata, node RETURN * ORDER BY id",
                MapUtil.map("host", HOST_WEAVIATE, "id1", ID_1, "id2", ID_2, "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "node")
        );
    }

    @Test
    public void queryVectorsWithCreateRel() {
        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true, 
                FIELDS_KEY, FIELDS,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                REL_TYPE, "TEST",
                ENTITY_KEY, "myId",
                METADATA_KEY, "foo"));
        testResult(db, "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, rel RETURN * ORDER BY id",
                map("host", HOST_WEAVIATE, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, ID_1, REL);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, ID_2, REL);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertRelsCreated(db);
    }

    @Test
    public void queryReadOnlyVectorsWithMapping() {
        db.executeTransactionally("CREATE (:Start)-[:TEST {readID: 'one'}]->(:End), (:Start)-[:TEST {readID: 'two'}]->(:End)");

        Map<String, Object> conf = MapUtil.map(ALL_RESULTS_KEY, true,
                FIELDS_KEY, FIELDS,
                HEADERS_KEY, READONLY_AUTHORIZATION,
                MAPPING_KEY, MapUtil.map(
                        REL_TYPE, "TEST",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo")
        );

        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, rel RETURN * ORDER BY id",
                MapUtil.map("host", HOST_WEAVIATE, "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "rel")
        );
    }

    @Test
    public void queryWithWrongEmbeddingSize() {
        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                FIELDS_KEY, FIELDS,
                HEADERS_KEY, READONLY_AUTHORIZATION);

        String expectedErrMsg = "distance between entrypoint and query node: vector lengths don't match: 4 vs 3";
        
        assertFails(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9], null, 5, $conf)",
                map("host", HOST_WEAVIATE, "conf", conf),
                expectedErrMsg);
    }

    @Test
    public void queryVectorsWithCreateRelWithoutVectorResult() {

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");

        Map<String, Object> conf = map(FIELDS_KEY, FIELDS,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, map(REL_TYPE, "TEST",
                    ENTITY_KEY, "myId",
                    METADATA_KEY, "foo")
        );
        testResult(db, "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, rel RETURN * ORDER BY id",
                map("host", HOST_WEAVIATE, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    Map<String, Object> props = ((Entity) row.get("rel")).getAllProperties();
                    assertEquals("Berlin", props.get("city"));
                    assertEquals("one", props.get("myId"));
                    assertNull(props.get("vect"));
                    
                    assertNotNull(row.get("score"));
                    assertNull(row.get("vector"));

                    row = r.next();
                    props = ((Entity) row.get("rel")).getAllProperties();
                    assertEquals("London", props.get("city"));
                    assertEquals("two", props.get("myId"));
                    assertNull(props.get("vect"));
                    
                    assertNotNull(row.get("score"));
                    assertNull(row.get("vector"));
                });
    }

    @Test
    public void queryVectorsWithSystemDbStorage() {
        String keyConfig = "weaviate-config-foo";
        String baseUrl = "http://" + HOST_WEAVIATE + "/v1";
        assertQueryVectorsWithSystemDbStorage(keyConfig, baseUrl, false);
    }

    @Test
    public void queryVectorsWithSystemDbStorageWithUrlWithoutVersion() {
        String keyConfig = "weaviate-config-foo";
        String baseUrl = "http://" + HOST_WEAVIATE;
        assertQueryVectorsWithSystemDbStorage(keyConfig, baseUrl, false);
    }

    @Test
    public void queryVectorsWithSystemDbStorageWithUrlV3Version() {
        String keyConfig = "weaviate-config-foo";
        String baseUrl = "http://" + HOST_WEAVIATE + "/v3";
        assertQueryVectorsWithSystemDbStorage(keyConfig, baseUrl, true);
    }

    @Test
    public void queryVectorsWithRag() {
        String openAIKey = ragSetup(db);

        Map<String, Object> conf = MapUtil.map(
                FIELDS_KEY, FIELDS,
                ALL_RESULTS_KEY, true,
                HEADERS_KEY, READONLY_AUTHORIZATION,
                MAPPING_KEY, MapUtil.map(EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Rag",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo")
        );

        testResult(db,
                """
                    CALL apoc.vectordb.weaviate.getAndUpdate($host, 'TestCollection', [$id1], $conf) YIELD score, node, metadata, id, vector
                    WITH collect(node) as paths
                    CALL apoc.ml.rag(paths, $attributes, "Which city has foo equals to one?", $confPrompt) YIELD value
                    RETURN value
                    """
                ,
                MapUtil.map(
                        "host", HOST_WEAVIATE,
                        "id1", ID_1,
                        "conf", conf,
                        "confPrompt", MapUtil.map(API_KEY_CONF, openAIKey),
                        "attributes", List.of("city", "foo")
                ),
                VectorDbTestUtil::assertRagWithVectors);
    }

    private static void assertQueryVectorsWithSystemDbStorage(String keyConfig, String baseUrl, boolean fails) {
        Map<String, String> mapping = map(EMBEDDING_KEY, "vect",
                NODE_LABEL, "Test",
                ENTITY_KEY, "myId",
                METADATA_KEY, "foo");
        sysDb.executeTransactionally("CALL apoc.vectordb.configure($vectorName, $keyConfig, $databaseName, $conf)",
                map("vectorName", WEAVIATE.toString(),
                        "keyConfig", keyConfig,
                        "databaseName", DEFAULT_DATABASE_NAME,
                        "conf", map(
                                "host", baseUrl,
                                "credentials", ADMIN_KEY,
                                "mapping", mapping
                        )
                )
        );

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        String query = "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)";
        Map<String, Object> params = map("host", keyConfig,
                "conf", map(FIELDS_KEY, FIELDS, ALL_RESULTS_KEY, true)
        );

        if (fails) {
            assertFails(
                    db,
                    query,
                    params,
                    "Caused by: java.io.FileNotFoundException: http://127.0.0.1:" + HOST_WEAVIATE.split(":")[1] + "/v3/graphql"
            );
            return;
        }


        testResult(db, query,
                params,
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, ID_1, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, ID_2, NODE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }

    @Test
    public void queryVectorsWithMetadataKeyNoFields() {
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata RETURN * ORDER BY id",
                map("host", HOST, "conf", map(
                        ALL_RESULTS_KEY, true,
                        MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                                NODE_LABEL, "Test",
                                ENTITY_KEY, "myId",
                                METADATA_KEY, "foo"
                        ),
                        HEADERS_KEY, ADMIN_AUTHORIZATION)),
                VectorDbTestUtil::assertMetadataFooResult);
    }

    @Test
    public void queryVectorsWithNoMetadataKeyNoFields() {
        Map<String, Object> params = map("host", HOST, "conf", map(
                ALL_RESULTS_KEY, true,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "myId"
                ),
                HEADERS_KEY, ADMIN_AUTHORIZATION));
        String query = "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                " YIELD score, vector, id, metadata RETURN * ORDER BY id";
        ExtendedTestUtil.assertFails(db, query, params, NO_FIELDS_ERROR_MSG);
    }

    @Test
    public void queryAndUpdateMetadataKeyWithoutFieldsTest() {
        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");
        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "myId",
                        METADATA_KEY, "foo"));
        testResult(db, "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                        " YIELD score, vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
                VectorDbTestUtil::assertMetadataFooResult);
    }

    @Test
    public void queryAndUpdateWithCreateNodeUsingExistingNodeFailWithNoMetadataKeyAndNoFields() {
        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");
        Map<String, Object> params = map("host", HOST,
                "conf", Map.of(ALL_RESULTS_KEY, true,
                    HEADERS_KEY, ADMIN_AUTHORIZATION,
                    MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "myId")));
        String query = "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) YIELD score, vector, id, metadata, node RETURN * ORDER BY id";
        ExtendedTestUtil.assertFails(db, query, params, NO_FIELDS_ERROR_MSG);
    }
}
