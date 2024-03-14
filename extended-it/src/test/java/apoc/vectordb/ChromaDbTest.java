package apoc.vectordb;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.chromadb.ChromaDBContainer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.vectordb.VectorDbTestUtil.assertBerlinVector;
import static apoc.vectordb.VectorDbTestUtil.assertLondonVector;
import static apoc.vectordb.VectorDbTestUtil.assertNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertOtherNodesCreated;
import static apoc.vectordb.VectorDbTestUtil.assertRelsAndIndexesCreated;
import static apoc.vectordb.VectorDbTestUtil.dropAndDeleteAll;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ChromaDbTest {
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();
    
    private static final ChromaDBContainer chroma = new ChromaDBContainer("chromadb/chroma:0.4.25.dev137");
    private static final AtomicReference<String> collId = new AtomicReference<>();
    
    public static String HOST;

    @BeforeClass
    public static void setUp() throws Exception {
        chroma.start();

        HOST = "localhost:" + chroma.getMappedPort(8000);
        TestUtil.registerProcedure(db, ChromaDb.class);
        
        testCall(db, "CALL apoc.vectordb.chroma.createCollection($host, 'test_collection', 'cosine', 4)",
            map("host", HOST),
                r -> {
                    Map value = (Map) r.get("value");
                    collId.set((String) value.get("id"));
                });

        testCall(db, """
                        CALL apoc.vectordb.chroma.upsert($host, $collection,
                        [
                            {id: '1', vector: [0.05, 0.61, 0.76, 0.74], metadata: {city: "Berlin", foo: "one"}, text: 'ajeje'},
                            {id: '2', vector: [0.19, 0.81, 0.75, 0.11], metadata: {city: "London", foo: "two"}, text: 'brazorf'}
                        ])
                        """,
                map("host", HOST, "collection", collId.get()),
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
    }

    @Before
    public void before() {
        dropAndDeleteAll(db);
    }

    @Test
    public void getEmbeddings() {
        testResult(db, "CALL apoc.vectordb.chroma.get($host, $collection, ['1']) ",
                Map.of("host", HOST, "collection", collId.get()),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("vector"));
                    assertEquals("ajeje", row.get("text"));
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
                map("host", HOST, "collection", collId.get()),
                r -> {
                    assertNull(r.get("value"));
                });

        testCall(db, "CALL apoc.vectordb.chroma.delete($host, $collection, [3]) ",
                Map.of("host", HOST, "collection", collId.get()),
                r -> {
                    assertEquals(List.of("3"), r.get("value"));
                });
    }

    @Test
    public void createAndDeleteVector() {
        testResult(db, "CALL apoc.vectordb.chroma.get($host, $collection, ['1']) ",
                Map.of("host", HOST, "collection", collId.get()),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinVector(row);
                    assertNotNull(row.get("vector"));
                });
    }

    @Test
    public void getEmbedding() {
        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5)",
                Map.of("host", HOST, "collection", collId.get(), "conf", emptyMap()),
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
        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5) YIELD metadata, id",
                Map.of("host", HOST, "collection", collId.get(), "conf", emptyMap()),
                r -> {
                    assertBerlinVector(r.next());
                    assertLondonVector(r.next());
                });
    }

    @Test
    public void getEmbeddingWithFilter() {
        testResult(db, """
                        CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {city: 'London'}, 5) YIELD metadata, id""",
                Map.of("host", HOST, "collection", collId.get(), "conf", emptyMap()),
                r -> {
                    assertLondonVector(r.next());
                });
    }

    @Test
    public void getEmbeddingWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 1) YIELD metadata, id""",
                Map.of("host", HOST, "collection", collId.get(), "conf", emptyMap()),
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
        
        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "collection", collId.get(), "conf", conf),
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


        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.22, 0.11, 0.99, 0.17], {}, 5, $conf) " +
                       "   YIELD score, vector, id, metadata RETURN * ORDER BY id",
                Map.of("host", HOST, "collection", collId.get(), "conf", conf),
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
        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "collection", collId.get(), "conf", conf),
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
                "id", "foo",
                "create", true));
        testResult(db, "CALL apoc.vectordb.chroma.query($host, $collection, [0.2, 0.1, 0.9, 0.7], {}, 5, $conf)",
                Map.of("host", HOST, "collection", collId.get(), "conf", conf),
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
