package apoc.vectordb;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.qdrant.QdrantContainer;

import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.vectordb.VectorDbTestUtil.assertBerlinResult;
import static apoc.vectordb.VectorDbTestUtil.assertLondonResult;
import static apoc.vectordb.VectorDbTestUtil.assertNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertRelsAndIndexesCreated;
import static apoc.vectordb.VectorDbTestUtil.dropAndDeleteAll;
import static apoc.vectordb.VectorEmbeddingConfig.ALL_RESULTS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class QdrantDbTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static final QdrantContainer qdrant = new QdrantContainer("qdrant/qdrant:v1.7.4");
    public static String HOST;

    @BeforeClass
    public static void setUp() throws Exception {
        qdrant.start();

        HOST = "localhost:" + qdrant.getMappedPort(6333);
        TestUtil.registerProcedure(db, Qdrant.class);

        testCall(db, "CALL apoc.vectordb.qdrant.createCollection($host, 'test_collection', 'Cosine', 4)",
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });

        testCall(db, """
                        CALL apoc.vectordb.qdrant.upsert($host, 'test_collection',
                        [
                            {id: 1, vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: "Berlin", foo: "one"}},
                            {id: 2, vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: "London", foo: "two"}}
                        ])
                        """,
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });

    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCall(db, "CALL apoc.vectordb.qdrant.deleteCollection($host, 'test_collection')",
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals(true, value.get("result"));
                });
    }
    
    @Before
    public void before() {
        dropAndDeleteAll(db);
    }
    
    @Test
    public void getVectors() {
        testResult(db, "CALL apoc.vectordb.qdrant.get($host, 'test_collection', [1], $conf) ",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, false);
                    assertNotNull(row.get("vector"));
                });
    }
    
    @Test
    public void getVectorsWithoutVectorResult() {
        testResult(db, "CALL apoc.vectordb.qdrant.get($host, 'test_collection', [1]) ",
                map("host", HOST),
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
                        ])
                        """,
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });
        
        testCall(db, "CALL apoc.vectordb.qdrant.delete($host, 'test_collection', [3, 4]) ",
                map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });
    }

    @Test
    public void queryVectors() {
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, false);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, false);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });
    }

    @Test
    public void queryVectorsWithoutVectorResult() {
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5)",
                map("host", HOST, "conf", emptyMap()),
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
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    assertBerlinResult(r.next(), false);
                    assertLondonResult(r.next(), false);
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
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    assertLondonResult(r.next(), false);
                });
    }

    @Test
    public void queryVectorsWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 1, $conf) YIELD metadata, id""",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    assertBerlinResult(r.next(), false);
                });
    }

    @Test
    public void queryVectorsWithCreateIndex() {

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, map("embeddingProp", "vect", 
                "label", "Test", 
                "prop", "myId", 
                "id", "foo",
                "create", true));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                VectorDbTestUtil::vectorEntityAssertions);

        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }

    @Test
    public void queryVectorsWithCreateIndexUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, map("embeddingProp", "vect",
                "label", "Test",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db);
    }

    @Test
    public void queryVectorsWithCreateRelIndex() {

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");
        
        Map<String, Object> conf = map(ALL_RESULTS_KEY, true,
                MAPPING_KEY, map("embeddingProp", "vect",
                "type", "TEST",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                map("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertRelsAndIndexesCreated(db);
    }

}
