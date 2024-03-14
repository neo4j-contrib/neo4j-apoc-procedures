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
import static apoc.vectordb.VectorDbTestUtil.assertBerlinVector;
import static apoc.vectordb.VectorDbTestUtil.assertLondonVector;
import static apoc.vectordb.VectorDbTestUtil.assertNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertOtherNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertRelsAndIndexesCreated;
import static apoc.vectordb.VectorDbTestUtil.dropAndDeleteAll;
import static apoc.vectordb.VectorDbTestUtil.vectorEntityAssertions;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
    public void getEmbeddings() {
        testResult(db, "CALL apoc.vectordb.qdrant.get($host, 'test_collection', [1]) ",
                Map.of("host", HOST),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("vector"));
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
                Map.of("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("ok", value.get("status"));
                });
    }

    @Test
    public void getEmbedding() {
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5)",
                Map.of("host", HOST, "conf", emptyMap()),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });
    }

    @Test
    public void getEmbeddingWithYield() {
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5) YIELD metadata, id",
                Map.of("host", HOST, "conf", emptyMap()),
                r -> {
                    assertBerlinVector(r.next());
                    assertLondonVector(r.next());
                });
    }

    @Test
    public void getEmbeddingWithFilter() {
        testResult(db, """
                        CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], 
                        { must: 
                            [ { key: "city", match: { value: "London" } } ] 
                        }, 
                        5) YIELD metadata, id""",
                Map.of("host", HOST, "conf", emptyMap()),
                r -> {
                    assertLondonVector(r.next());
                });
    }

    @Test
    public void getEmbeddingWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 1) YIELD metadata, id""",
                Map.of("host", HOST, "conf", emptyMap()),
                r -> {
                    assertBerlinVector(r.next());
                });
    }

    @Test
    public void getEmbeddingWithCreateIndex() {

        Map<String, Object> conf = Map.of(MAPPING_KEY, Map.of("embeddingProp", "vect", 
                "label", "Test", 
                "prop", "myId", 
                "id", "foo",
                "create", true));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db, true);

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                r -> vectorEntityAssertions(r, true));

        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertOtherNodesCreated(db);
    }

    @Test
    public void getEmbeddingWithCreateIndexUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = Map.of(MAPPING_KEY, Map.of("embeddingProp", "vect",
                "label", "Test",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db, false);
    }

    @Test
    public void getEmbeddingWithCreateRelIndex() {

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");
        
        Map<String, Object> conf = Map.of(MAPPING_KEY, Map.of("embeddingProp", "vect",
                "type", "TEST",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.qdrant.query($host, 'test_collection', [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonVector(row);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertRelsAndIndexesCreated(db);
    }

}
