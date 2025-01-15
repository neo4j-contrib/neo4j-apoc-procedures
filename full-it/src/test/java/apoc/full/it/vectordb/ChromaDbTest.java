package apoc.full.it.vectordb;

import apoc.ml.Prompt;
import apoc.util.TestUtil;
import apoc.vectordb.ChromaDb;
import apoc.vectordb.VectorDb;
import apoc.vectordb.VectorDbTestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.testcontainers.chromadb.ChromaDBContainer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static apoc.ml.Prompt.API_KEY_CONF;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.vectordb.VectorDbHandler.Type.CHROMA;
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
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorMappingConfig.EMBEDDING_KEY;
import static apoc.vectordb.VectorMappingConfig.ENTITY_KEY;
import static apoc.vectordb.VectorMappingConfig.METADATA_KEY;
import static apoc.vectordb.VectorMappingConfig.MODE_KEY;
import static apoc.vectordb.VectorMappingConfig.MappingMode;
import static apoc.vectordb.VectorMappingConfig.NODE_LABEL;
import static apoc.vectordb.VectorMappingConfig.REL_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class ChromaDbTest {
    private static final AtomicReference<String> COLL_ID = new AtomicReference<>();
    private static final ChromaDBContainer CHROMA_CONTAINER = new ChromaDBContainer("chromadb/chroma:0.4.25.dev137");
    private static final String READONLY_KEY = "my_readonly_api_key";
    private static final Map<String, String> READONLY_AUTHORIZATION = getAuthHeader(READONLY_KEY);
    private static final String COLLECTION_NAME = "test_collection";

    private static String HOST;

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
        
        CHROMA_CONTAINER.start();

        HOST = CHROMA_CONTAINER.getEndpoint();
        TestUtil.registerProcedure(db, ChromaDb.class, VectorDb.class, Prompt.class);
        
        testCall(db, "CALL apoc.vectordb.chroma.createCollection($host, 'test_collection', 'cosine', 4)",
            map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    COLL_ID.set((String) value.get("id"));
                });

        testCall(db, """
                        CALL apoc.vectordb.chroma.upsert($host, $collection,
                        [
                            {id: '1', vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: "Berlin", foo: "one"}, text: 'ajeje'},
                            {id: '2', vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: "London", foo: "two"}, text: 'brazorf'}
                        ])
                        """,
                map("host", HOST, "collection", COLL_ID.get()),
                r -> {
                    assertNull(r.get("value"));
                });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCall(db, "CALL apoc.vectordb.chroma.deleteCollection($host, 'test_collection')",
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertNull(value);
                });

        databaseManagementService.shutdown();
        CHROMA_CONTAINER.stop();
    }

    @Before
    public void before() {
        dropAndDeleteAll(db);
    }

    @Test
    public void getInfo() {
        testResult(db, "CALL apoc.vectordb.chroma.info($host, $collection, $conf) ",
                map("host", HOST, "collection", COLLECTION_NAME, "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    Map<String, Object> row = (Map<String, Object>) r.next().get("value");
                    assertEquals(COLLECTION_NAME, row.get("name"));
                });
    }

    @Test
    public void getInfoNotExistentCollection() {
        assertFails(db, "CALL apoc.vectordb.chroma.info($host, 'wrong_collection', $conf) ",
                map("host", HOST, "collection", COLLECTION_NAME, "conf", map(ALL_RESULTS_KEY, true)),
                "Server returned HTTP response code: 500"
        );
    }
    
    @Test
    public void getVectors() {
        testResult(db, "CALL apoc.vectordb.chroma.get($host, $collection, ['1'], $conf) ",
                map("host", HOST, "collection", COLL_ID.get(), "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, FALSE);
                    assertNotNull(row.get("vector"));
                    assertEquals("ajeje", row.get("text"));
                });
    }

    @Test
    public void getVectorsWithoutVectorResult() {
        testResult(db, "CALL apoc.vectordb.chroma.get($host, $collection, ['1'])",
                map("host", HOST, "collection", COLL_ID.get()),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
                    assertNull(row.get("vector"));
                    assertNull(row.get("id"));
                });
    }

    @Test
    public void deleteVector() {
        testCall(db, """
                        CALL apoc.vectordb.chroma.upsert($host, $collection,
                        [
                            {id: 3, embedding: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}}
                        ])
                        """,
                map("host", HOST, "collection", COLL_ID.get()),
                r -> {
                    assertNull(r.get("value"));
                });

        testCall(db, "CALL apoc.vectordb.chroma.delete($host, $collection, [3]) ",
                map("host", HOST, "collection", COLL_ID.get()),
                r -> {
                    assertEquals(List.of("3"), r.get("value"));
                });
    }

    @Test
    public void createAndDeleteVector() {
        testResult(db, "CALL apoc.vectordb.chroma.get($host, $collection, ['1'], $conf) ",
                map("host", HOST, "collection", COLL_ID.get(), "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, FALSE);
                    assertNotNull(row.get("vector"));
                });
    }

    @Test
    public void queryVectors() {
        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "collection", COLL_ID.get(), "conf", map(ALL_RESULTS_KEY, true)),
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
        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5) " +
                       " YIELD score, vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST, "collection", COLL_ID.get()),
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
        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf) YIELD metadata, id",
                map("host", HOST, "collection", COLL_ID.get(), "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    assertBerlinResult(r.next(), FALSE);
                    assertLondonResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithFilter() {
        testResult(db, """
                        CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {city: 'London'}, 5, $conf) YIELD metadata, id""",
                map("host", HOST, "collection", COLL_ID.get(), "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    assertLondonResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 1, $conf) YIELD metadata, id""",
                map("host", HOST, "collection", COLL_ID.get(), "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    assertBerlinResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithCreateNode() {
        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                    NODE_LABEL, "Test",
                    ENTITY_KEY, "myId",
                    METADATA_KEY, "foo",
                    MODE_KEY, MappingMode.CREATE_IF_MISSING.toString()
                )
        );
        
        testResult(db, "CALL apoc.vectordb.chroma.queryAndUpdate($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "collection", COLL_ID.get(), "conf", conf),
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

        testResult(db, "CALL apoc.vectordb.chroma.queryAndUpdate($host, $collection, [0.22, 0.11, 0.99, 0.17], {}, 5, $conf) " +
                       "   YIELD score, vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST, "collection", COLL_ID.get(), "conf", conf),
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

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "myId",
                        METADATA_KEY, "foo"));

        testResult(db, "CALL apoc.vectordb.chroma.getAndUpdate($host, $collection, ['1', '2'], $conf)",
                map("host", HOST, "collection", COLL_ID.get(), "conf", conf),
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

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, map(NODE_LABEL, "Test",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo")
        );

        testResult(db, "CALL apoc.vectordb.chroma.get($host, $collection, ['1', '2'], $conf) " +
                       "YIELD vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST, "collection", COLL_ID.get(), "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "node")
        );
    }
    
    @Test
    public void queryVectorsWithCreateNodeUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "myId",
                        METADATA_KEY, "foo"));
        testResult(db, "CALL apoc.vectordb.chroma.queryAndUpdate($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "collection", COLL_ID.get(), "conf", conf),
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
    public void queryReadOnlyVectorsWithMapping() {
        db.executeTransactionally("CREATE (:Start)-[:TEST {readID: 'one'}]->(:End), (:Start)-[:TEST {readID: 'two'}]->(:End)");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, map(
                        REL_TYPE, "TEST",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo")
        );

        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "collection", COLL_ID.get(), "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "rel")
        );
    }

    @Test
    public void queryVectorsWithCreateRel() {

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                REL_TYPE, "TEST",
                ENTITY_KEY, "myId",
                METADATA_KEY, "foo",
                "create", true));
        testResult(db, "CALL apoc.vectordb.chroma.queryAndUpdate($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "collection", COLL_ID.get(), "conf", conf),
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
    public void queryVectorsWithSystemDbStorage() {
        String keyConfig = "chroma-config-foo";
        String baseUrl = HOST;
        Map<String, Object> mapping = map(EMBEDDING_KEY, "vect",
                NODE_LABEL, "Test",
                ENTITY_KEY, "myId",
                METADATA_KEY, "foo");
        sysDb.executeTransactionally("CALL apoc.vectordb.configure($vectorName, $keyConfig, $databaseName, $conf)",
                map("vectorName", CHROMA.toString(),
                        "keyConfig", keyConfig,
                        "databaseName", DEFAULT_DATABASE_NAME,
                        "conf", map(
                                "host", baseUrl,
                                "credentials", null,
                                "mapping", mapping
                        )
                )
        );

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        testResult(db, "CALL apoc.vectordb.chroma.queryAndUpdate($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", keyConfig, "collection", COLL_ID.get(), "conf", map(ALL_RESULTS_KEY, true)),
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

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, READONLY_AUTHORIZATION,
                MAPPING_KEY, map(NODE_LABEL, "Rag",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo")
        );

        testResult(db,
                """
                    CALL apoc.vectordb.chroma.getAndUpdate($host, $collection, ['1', '2'], $conf) YIELD node, metadata, id, vector
                    WITH collect(node) as paths
                    CALL apoc.ml.rag(paths, $attributes, "Which city has foo equals to one?", $confPrompt) YIELD value
                    RETURN value
                    """
                ,
                map(
                        "host", HOST,
                        "conf", conf,
                        "collection", COLL_ID.get(),
                        "confPrompt", map(API_KEY_CONF, openAIKey),
                        "attributes", List.of("city", "foo")
                ),
                VectorDbTestUtil::assertRagWithVectors);
    }
}
