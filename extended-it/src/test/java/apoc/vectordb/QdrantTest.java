package apoc.vectordb;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.testcontainers.qdrant.QdrantContainer;

import java.util.List;
import java.util.Map;

import apoc.ml.Prompt;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.ml.Prompt.API_KEY_CONF;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.vectordb.VectorDbHandler.Type.QDRANT;
import static apoc.vectordb.VectorDbTestUtil.EntityType.NODE;
import static apoc.vectordb.VectorDbTestUtil.EntityType.FALSE;
import static apoc.vectordb.VectorDbTestUtil.EntityType.REL;
import static apoc.vectordb.VectorDbTestUtil.assertBerlinResult;
import static apoc.vectordb.VectorDbTestUtil.assertLondonResult;
import static apoc.vectordb.VectorDbTestUtil.assertNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertReadOnlyProcWithMappingResults;
import static apoc.vectordb.VectorDbTestUtil.assertRelsCreated;
import static apoc.vectordb.VectorDbTestUtil.dropAndDeleteAll;
import static apoc.vectordb.VectorDbTestUtil.getAuthHeader;
import static apoc.vectordb.VectorEmbeddingConfig.ALL_RESULTS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorMappingConfig.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.*;

public class QdrantTest {
    private static final String ADMIN_KEY = "my_admin_api_key";
    private static final String READONLY_KEY = "my_readonly_api_key";
    
    private static final QdrantContainer QDRANT_CONTAINER = new QdrantContainer("qdrant/qdrant:v1.7.4")
            .withEnv("QDRANT__SERVICE__API_KEY", ADMIN_KEY)
            .withEnv("QDRANT__SERVICE__READ_ONLY_API_KEY", READONLY_KEY);

    private static final Map<String, String> ADMIN_AUTHORIZATION = getAuthHeader(ADMIN_KEY);
    private static final Map<String, String> READONLY_AUTHORIZATION = getAuthHeader(READONLY_KEY);
    private static final Map<String, Object> ADMIN_HEADER_CONF  = map(HEADERS_KEY, ADMIN_AUTHORIZATION);

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
        
        QDRANT_CONTAINER.start();

        HOST = QDRANT_CONTAINER.getHost() + ":" +QDRANT_CONTAINER.getMappedPort(6333);
        TestUtil.registerProcedure(db, Qdrant.class, VectorDb.class, Prompt.class);

        testCall(db, "CALL apoc.vectordb.qdrant.createCollection($host, 'test_collection', 'Cosine', 4, $conf)",
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });

        testCall(db, """
                        CALL apoc.vectordb.qdrant.upsert($host, 'test_collection',
                        [
                            {id: 1, vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: "Berlin", foo: "one"}},
                            {id: 2, vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: "London", foo: "two"}}
                        ],
                        $conf)
                        """,
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCall(db, "CALL apoc.vectordb.qdrant.deleteCollection($host, 'test_collection', $conf)",
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals(true, value.get("result"));
                });

        databaseManagementService.shutdown();
        QDRANT_CONTAINER.stop();
    }
    
    @Before
    public void before() {
        dropAndDeleteAll(db);
    }
    
    @Test
    public void getVectorsWithReadOnlyApiKey() {
        testResult(db, "CALL apoc.vectordb.qdrant.get($host, 'test_collection', [1], $conf) ",
                map("host", HOST, 
                        "conf", map(ALL_RESULTS_KEY, true, HEADERS_KEY, READONLY_AUTHORIZATION)
                ),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, FALSE);
                    assertNotNull(row.get("vector"));
                });
    }

    @Test
    public void writeOperationWithReadOnlyUser() {
        try {
            testCall(db, "CALL apoc.vectordb.qdrant.deleteCollection($host, 'test_collection', $conf)",
                    Util.map("host", HOST,
                            "conf", Util.map(HEADERS_KEY, READONLY_AUTHORIZATION)
                    ),
                    r -> fail()
            );
        } catch (Exception e) {
            assertThat( e.getMessage() ).contains("HTTP response code: 403");
        }
    }
    
    @Test
    public void getVectorsWithoutVectorResult() {
        testResult(db, "CALL apoc.vectordb.qdrant.get($host, 'test_collection', [1], $conf) ",
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
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
                        CALL apoc.vectordb.qdrant.upsert($host, 'test_collection',
                        [
                            {id: 3, vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}},
                            {id: 4, vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}}
                        ],
                        $conf)
                        """,
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });
        
        testCall(db, "CALL apoc.vectordb.qdrant.delete($host, 'test_collection', [3, 4], $conf) ",
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });
    }

    @Test
    public void queryVectorsWithRag() {
        String openAIKey = System.getenv("OPENAI_KEY");;
        Assume.assumeNotNull("No OPENAI_KEY environment configured", openAIKey);

        db.executeTransactionally("CREATE (:Rag {readID: 'one'}), (:Rag {readID: 'two'})");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, READONLY_AUTHORIZATION,
                MAPPING_KEY, map(NODE_LABEL, "Rag",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo")
        );

        testResult(db,
                """
                    CALL apoc.vectordb.qdrant.getAndUpdate($host, 'test_collection', [1, 2], $conf) YIELD node, metadata, id, vector
                    WITH collect(node) as paths
                    CALL apoc.ml.rag(paths, $attributes, "Which city has foo equals to one?", $confPrompt) YIELD value
                    RETURN value
                     """
                ,
                map(
                        "host", HOST,
                        "conf", conf,
                        "confPrompt", map(API_KEY_CONF, openAIKey),
                        "attributes", List.of("city", "foo")
                ),
                r -> {
                    Map<String, Object> row = r.next();
                    Object value = row.get("value");
                    assertTrue("The actual value is: " + value, value.toString().contains("Berlin"));
                });
    }

    @Test
    public void queryVectors() {
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)),
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
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "conf", map(HEADERS_KEY, ADMIN_AUTHORIZATION)),
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
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf) YIELD metadata, id",
                map("host", HOST,
                        "conf", map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)
                ),
                r -> {
                    assertBerlinResult(r.next(), FALSE);
                    assertLondonResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithFilter() {
        testResult(db, """
                        CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7],
                        { must:
                            [ { key: "city", match: { value: "London" } } ]
                        },
                        5, $conf) YIELD metadata, id""",
                map("host", HOST,
                        "conf", map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)
                ),
                r -> {
                    assertLondonResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 1, $conf) YIELD metadata, id""",
                map("host", HOST,
                        "conf", map(ALL_RESULTS_KEY, true, HEADERS_KEY, ADMIN_AUTHORIZATION)
                ),
                r -> {
                    assertBerlinResult(r.next(), FALSE);
                });
    }

    @Test
    public void queryVectorsWithCreateNode() {

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, map(
                        EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "myId",
                        METADATA_KEY, "foo",
                        MODE_KEY, MappingMode.CREATE_IF_MISSING.toString()
                )
        );
        testResult(db, "CALL apoc.vectordb.qdrant.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
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

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                VectorDbTestUtil::vectorEntityAssertions);

        testResult(db, "CALL apoc.vectordb.qdrant.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
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

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "myId",
                        METADATA_KEY, "foo"));

        testResult(db, "CALL apoc.vectordb.qdrant.getAndUpdate($host, 'test_collection', [1, 2], $conf) " +
                       "YIELD vector, id, metadata, node RETURN * ORDER BY id",
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

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, READONLY_AUTHORIZATION,
                MAPPING_KEY, map(NODE_LABEL, "Test",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo")
        );

        testResult(db, "CALL apoc.vectordb.qdrant.get($host, 'test_collection', [1, 2], $conf) " +
                       "YIELD vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "node")
        );
    }

    @Test
    public void queryVectorsWithCreateNodeUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, map(EMBEDDING_KEY, "vect",
                        NODE_LABEL, "Test",
                        ENTITY_KEY, "myId",
                        METADATA_KEY, "foo"));
        
        testResult(db, "CALL apoc.vectordb.qdrant.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
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

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");
        
        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, ADMIN_AUTHORIZATION,
                MAPPING_KEY, map(
                        EMBEDDING_KEY, "vect",
                        REL_TYPE, "TEST",
                        ENTITY_KEY, "myId",
                        METADATA_KEY, "foo")
        );
        testResult(db, "CALL apoc.vectordb.qdrant.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
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
        db.executeTransactionally("CREATE (:Start)-[:TEST {readID: 'one'}]->(:End), (:Start)-[:TEST {readID: 'two'}]->(:End)");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                HEADERS_KEY, READONLY_AUTHORIZATION,
                MAPPING_KEY, map(
                        REL_TYPE, "TEST",
                        ENTITY_KEY, "readID",
                        METADATA_KEY, "foo")
        );

        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "conf", conf),
                r -> assertReadOnlyProcWithMappingResults(r, "rel")
        );
    }

    @Test
    public void queryVectorsWithSystemDbStorage() {
        String keyConfig = "qdrant-config-foo";
        String baseUrl = "http://" + HOST;
        Map<String, Object> mapping = map(EMBEDDING_KEY, "vect",
                NODE_LABEL, "Test",
                ENTITY_KEY, "myId",
                METADATA_KEY, "foo");
        sysDb.executeTransactionally("CALL apoc.vectordb.configure($vectorName, $keyConfig, $databaseName, $conf)", 
                map("vectorName", QDRANT.toString(),
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

        testResult(db, "CALL apoc.vectordb.qdrant.queryAndUpdate($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", keyConfig, "conf", map(ALL_RESULTS_KEY, true)),
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
    
}
