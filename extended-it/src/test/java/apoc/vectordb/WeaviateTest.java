package apoc.vectordb;

import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.weaviate.WeaviateContainer;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static apoc.vectordb.VectorDbTestUtil.*;
import static apoc.vectordb.VectorEmbeddingConfig.ALL_RESULTS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


public class WeaviateTest {
    public static final List<String> FIELDS = List.of("city", "foo");
    
    private static String HOST;

    private static final WeaviateContainer weaviate = new WeaviateContainer("semitechnologies/weaviate:1.24.5");

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();
    
    private static final String id1 = "8ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";
    private static final String id2 = "9ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308";

    @BeforeClass
    public static void setUp() throws Exception {
        weaviate.start();
        HOST = weaviate.getHttpHostAddress();

        TestUtil.registerProcedure(db, Weaviate.class);

        testCall(db, "CALL apoc.vectordb.weaviate.createCollection($host, 'TestCollection', 'cosine', 4)",
                MapUtil.map("host", HOST),
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
                        ])
                        """,
                MapUtil.map("host", HOST, "id1", id1, "id2", id2),
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
                     "['7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308', '7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309']) ",
                map("host", HOST),
                r -> {
                    List value = (List) r.get("value");
                    assertEquals(List.of("7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308", "7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309"), value);
                });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCallEmpty(db, "CALL apoc.vectordb.weaviate.deleteCollection($host, 'TestCollection')",
                MapUtil.map("host", HOST)
        );
    }

    @Before
    public void before() {
        dropAndDeleteAll(db);
    }

    @Test
    public void getVectors() {
        testResult(db, "CALL apoc.vectordb.weaviate.get($host, 'TestCollection', [$id1], $conf)",
                map("host", HOST, "id1", id1, "conf", map(ALL_RESULTS_KEY, true)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, id1, false);
                    assertNotNull(row.get("vector"));
                });
    }
    
    @Test
    public void getVectorsWithoutVectorResult() {
        testResult(db, "CALL apoc.vectordb.weaviate.get($host, 'TestCollection', [$id1])",
                map("host", HOST, "id1", id1),
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
                       " YIELD score, vector, id, metadata, entity RETURN * ORDER BY id",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, "fields", FIELDS)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, id1, false);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, id2, false);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });
    }

    @Test
    public void queryVectorsWithoutVectorResult() {
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, entity RETURN * ORDER BY id",
                map("host", HOST, "conf", map( "fields", FIELDS)),
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
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, "fields", FIELDS)),
                r -> {
                    assertBerlinResult(r.next(), id1, false);
                    assertLondonResult(r.next(), id2, false);
                });
    }

    @Test
    public void queryVectorsWithFilter() {
        testResult(db, """
                        CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7],
                        '{operator: Equal, valueString: "London", path: ["city"]}',
                        5, $conf) YIELD metadata, id RETURN * ORDER BY id""",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, "fields", FIELDS)),
                r -> {
                    assertLondonResult(r.next(), id2, false);
                });
    }

    @Test
    public void queryVectorsWithLimit() {
        testResult(db, """
                        CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 1, $conf) YIELD metadata, id RETURN * ORDER BY id""",
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, "fields", FIELDS)),
                r -> {
                    assertBerlinResult(r.next(), id1, false);
                });
    }

    @Test
    public void queryVectorsWithCreateIndex() {

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true, 
                "fields", FIELDS,
                MAPPING_KEY, map("embeddingProp", "vect",
                "label", "Test",
                "prop", "myId",
                "id", "foo",
                "create", true));
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       "YIELD score, vector, id, metadata, entity RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, id1, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, id2, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db, true);

        testResult(db, "MATCH (n:Test) RETURN properties(n) AS props ORDER BY n.myId",
                r -> vectorEntityAssertions(r, true));

        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, entity RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, id1, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, id2, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db, true);
    }

    @Test
    public void queryVectorsWithCreateIndexUsingExistingNode() {

        db.executeTransactionally("CREATE (:Test {myId: 'one'}), (:Test {myId: 'two'})");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true, 
                "fields", FIELDS,
                MAPPING_KEY, map("embeddingProp", "vect",
                "label", "Test",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, entity RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, id1, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, id2, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertNodesCreated(db, false);
    }

    @Test
    public void queryVectorsWithCreateRelIndex() {

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");

        Map<String, Object> conf = map(ALL_RESULTS_KEY, true, 
                "fields", FIELDS,
                MAPPING_KEY, map("embeddingProp", "vect",
                "type", "TEST",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, entity RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertBerlinResult(row, id1, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));

                    row = r.next();
                    assertLondonResult(row, id2, true);
                    assertNotNull(row.get("score"));
                    assertNotNull(row.get("vector"));
                });

        assertRelsAndIndexesCreated(db);
    }

    @Test
    public void queryVectorsWithCreateRelIndexWithoutVectorResult() {

        db.executeTransactionally("CREATE (:Start)-[:TEST {myId: 'one'}]->(:End), (:Start)-[:TEST {myId: 'two'}]->(:End)");

        Map<String, Object> conf = map("fields", FIELDS,
                MAPPING_KEY, map("type", "TEST",
                "prop", "myId",
                "id", "foo"));
        testResult(db, "CALL apoc.vectordb.weaviate.query($host, 'TestCollection', [0.2, 0.1, 0.9, 0.7], null, 5, $conf) " +
                       " YIELD score, vector, id, metadata, entity RETURN * ORDER BY id",
                map("host", HOST, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    Map<String, Object> props = ((Entity) row.get("entity")).getAllProperties();
                    assertEquals("Berlin", props.get("city"));
                    assertEquals("one", props.get("myId"));
                    assertNull(props.get("vect"));
                    
                    assertNotNull(row.get("score"));
                    assertNull(row.get("vector"));

                    row = r.next();
                    props = ((Entity) row.get("entity")).getAllProperties();
                    assertEquals("London", props.get("city"));
                    assertEquals("two", props.get("myId"));
                    assertNull(props.get("vect"));
                    
                    assertNotNull(row.get("score"));
                    assertNull(row.get("vector"));
                });
    }

}
