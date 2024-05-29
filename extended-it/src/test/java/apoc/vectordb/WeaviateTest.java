package apoc.vectordb;

import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.assertj.core.api.Assertions;
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
import org.testcontainers.weaviate.WeaviateContainer;

import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static apoc.vectordb.VectorDbHandler.Type.WEAVIATE;
import static apoc.vectordb.VectorDbTestUtil.*;
import static apoc.vectordb.VectorDbTestUtil.EntityType.*;
import static apoc.vectordb.VectorDbUtil.ERROR_READONLY_MAPPING;
import static apoc.vectordb.VectorEmbeddingConfig.ALL_RESULTS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.FIELDS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorMappingConfig.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;


public class WeaviateTest {
    private static final List<String> FIELDS = List.of("city", "foo");
    private static final String ADMIN_KEY = "jane-secret-key";
    private static final String READONLY_KEY = "ian-secret-key";

    private static final WeaviateContainer WEAVIATE_CONTAINER = new WeaviateContainer("semitechnologies/weaviate:1.24.5")
            .withEnv("AUTHENTICATION_APIKEY_ENABLED", "true")
            .withEnv("AUTHENTICATION_APIKEY_ALLOWED_KEYS", ADMIN_KEY + "," + READONLY_KEY)
            .withEnv("AUTHENTICATION_APIKEY_USERS", "jane@doe.com,ian-smith")
            
            .withEnv("AUTHORIZATION_ADMINLIST_ENABLED", "true")
            .withEnv("AUTHORIZATION_ADMINLIST_USERS", "jane@doe.com,john@doe.com")
            .withEnv("AUTHORIZATION_ADMINLIST_READONLY_USERS", "ian-smith,roberta@doe.com");

    private static final Map<String, String> ADMIN_AUTHORIZATION = getAuthHeader(ADMIN_KEY);
    private static final Map<String, String> READONLY_AUTHORIZATION = getAuthHeader(READONLY_KEY);
    private static final Map<String, Object> ADMIN_HEADER_CONF = map(HEADERS_KEY, ADMIN_AUTHORIZATION);
    
    private static final String ID_1 = "8ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";
    private static final String ID_2 = "9ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";
    
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
        
        WEAVIATE_CONTAINER.start();
        HOST = WEAVIATE_CONTAINER.getHttpHostAddress();

        TestUtil.registerProcedure(db, Weaviate.class, VectorDb.class);

        testCall(db, "CALL apoc.vectordb.weaviate.createCollection($host, 'TestCollection', 'cosine', 4, $conf)",
                MapUtil.map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("TestCollection", value.get("class"));
                });

        testResult(db, """
                        CALL apoc.vectordb.weaviate.upsert($host, 'TestCollection',
                        [
                            {id: $id1, vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: "Berlin", foo: "one"}},
                            {id: $id2, vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: "London", foo: "two"}},
                            {id: '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}},
                            {id: '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309', vector: [0.19, 0.81, 0.75, 0.11], metadata: {foo: "baz"}}
                        ],
                        $conf)
                        """,
                MapUtil.map("host", HOST, "id1", ID_1, "id2", ID_2, "conf", ADMIN_HEADER_CONF),
                r -> {
                    ResourceIterator<Map> values = r.columnAs("value");
                    assertEquals("TestCollection", values.next().get("class"));
                    assertEquals("TestCollection", values.next().get("class"));
                    assertEquals("TestCollection", values.next().get("class"));
                    assertEquals("TestCollection", values.next().get("class"));
                    assertFalse(values.hasNext());
                });
        
        // -- delete vector
        testCall(db, "CALL apoc.vectordb.weaviate.delete($host, 'TestCollection', " +
                     "['7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308', '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309']" +
                     ", $conf) ",
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    List value = (List) r.get("value");
                    assertEquals(List.of("7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308", "7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309"), value);
                });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCallEmpty(db, "CALL apoc.vectordb.weaviate.deleteCollection($host, 'TestCollection', $conf)",
                MapUtil.map("host", HOST, "conf", ADMIN_HEADER_CONF)
        );

        WEAVIATE_CONTAINER.stop();
        databaseManagementService.shutdown();
    }

    @Before
    public void before() {
        dropAndDeleteAll(db);
    }

    @Test
    public void getVectorsWithReadOnlyApiKey() {
        testResult(db, "CALL apoc.vectordb.weaviate.get($host, 'TestCollection', [$id1], $conf)",
                map("host", HOST, "id1", ID_1, "conf", map(ALL_RESULTS_KEY, true, HEADERS_KEY, READONLY_AUTHORIZATION)),
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
                    map("host", HOST, 
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
                map("host", HOST, "id1", ID_1, "conf", map(HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(Map.of("city", "Berlin", "foo", "one"), row.get("metadata"));
                    assertNull(row.get("vector"));
                    assertNull(row.get("id"));
                });
    }

    @Test
    public void queryVectors() {
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata RETURN * ORDER BY id",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, ID_1, FALSE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, ID_2, FALSE);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                }); 
    }

    @Test
    public void queryVectorsWithoutVectorResult() {
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST, "conf", map( FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
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
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
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
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                r -> {
                    assertLondonResult(r.next(), ID_2, FALSE);
                });
    }

    @Test
    public void queryVectorsWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 1, $conf) YIELD metadata, id RETURN * ORDER BY id""",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
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
                    CREATE_KEY, true)
        );
        testResult(db, "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       "YIELD score, vector, id, metadata, node RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
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
                map("host", HOST, "conf", conf),
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
                map("host", HOST, "conf", conf),
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
                map("host", HOST, "id1", ID_1, "id2", ID_2, "conf", conf),
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
        Map<String, Object> conf = MapUtil.map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, MapUtil.map(EMBEDDING_KEY, "vect"));

        try {
            testCall(db, "CALL apoc.vectordb.weaviate.get($host, 'TestCollection', [1, 2], $conf)",
                    map("host", HOST, "conf", conf),
                    r -> fail()
            );
        } catch (RuntimeException e) {
            Assertions.assertThat(e.getMessage()).contains(ERROR_READONLY_MAPPING);
        }
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
                map("host", HOST, "conf", conf),
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
        Map<String, Object> conf = MapUtil.map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, MapUtil.map(EMBEDDING_KEY, "vect"));

        try {
            testCall(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                    MapUtil.map("host", HOST, "conf", conf),
                    r -> fail()
            );
        } catch (RuntimeException e) {
            Assertions.assertThat(e.getMessage()).contains(ERROR_READONLY_MAPPING);
        }
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
                map("host", HOST, "conf", conf),
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
        String baseUrl = "http://" + HOST + "/v1";
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

        testResult(db, "CALL apoc.vectordb.weaviate.queryAndUpdate($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf)",
                map("host", keyConfig,
                        "conf", map(FIELDS_KEY, FIELDS, ALL_RESULTS_KEY, true)
                ),
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
}
