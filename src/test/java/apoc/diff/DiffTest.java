package apoc.diff;

import apoc.bolt.Bolt;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.time.OffsetTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static apoc.util.TestUtil.isTravis;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

/**
 * @author Benjamin Clauss
 * @since 15.06.2018
 */
public class DiffTest {

    private static Node node1;
    private static Node node2;
    private static Node node3;

    private GraphDatabaseService db;

    private static Neo4jContainerExtension neo4jContainer;

    @BeforeClass
    public static void setup() throws Exception {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            neo4jContainer = new Neo4jContainerExtension()
                    .withInitScript("init_neo4j_diff.cypher")
                    .withLogging()
                    .withoutAuthentication();
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
    }

    @AfterClass
    public static void tearDown() {
        if (neo4jContainer != null) {
            neo4jContainer.close();

        }
    }

    @Before
    public void before() throws KernelException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
        TestUtil.registerProcedure(db, Bolt.class, Diff.class);
    }

    @After
    public void after() {
        db.shutdown();
    }

    private void createNodes() {
        try (Transaction tx = db.beginTx()) {
            node1 = db.createNode();
            node1.setProperty("prop1", "val1");
            node1.setProperty("prop2", 2L);

            node2 = db.createNode();
            node2.setProperty("prop1", "val1");
            node2.setProperty("prop2", 2L);
            node2.setProperty("prop4", "four");

            node3 = db.createNode();
            node3.setProperty("prop1", "val1");
            node3.setProperty("prop3", "3");
            node3.setProperty("prop4", "for");
            tx.success();
        }
    }

    @Test
    public void nodesSame() {
        createNodes();
        Map<String, Object> params = new HashMap<>();
        params.put("leftNode", node1);
        params.put("rightNode", node1);

        Map<String, Object> result =
                (Map<String, Object>) db.execute(
                        "RETURN apoc.diff.nodes($leftNode, $rightNode) as diff", params).next().get("diff");

        assertNotNull(result);

        HashMap<String, Object> leftOnly = (HashMap<String, Object>) result.get("leftOnly");
        assertTrue(leftOnly.isEmpty());

        HashMap<String, Object> rightOnly = (HashMap<String, Object>) result.get("rightOnly");
        assertTrue(rightOnly.isEmpty());

        HashMap<String, Object> different = (HashMap<String, Object>) result.get("different");
        assertTrue(different.isEmpty());

        HashMap<String, Object> inCommon = (HashMap<String, Object>) result.get("inCommon");
        assertEquals(2, inCommon.size());
        assertEquals("val1", inCommon.get("prop1"));
        assertEquals(2L, inCommon.get("prop2"));
    }

    @Test
    public void nodesDiffering() {
        createNodes();
        Map<String, Object> params = new HashMap<>();
        params.put("leftNode", node2);
        params.put("rightNode", node3);
        Map<String, Object> result =
                (Map<String, Object>) db.execute(
                        "RETURN apoc.diff.nodes($leftNode, $rightNode) as diff", params).next().get("diff");

        assertNotNull(result);

        HashMap<String, Object> leftOnly = (HashMap<String, Object>) result.get("leftOnly");
        assertEquals(1, leftOnly.size());
        assertEquals(2L, leftOnly.get("prop2"));

        HashMap<String, Object> rightOnly = (HashMap<String, Object>) result.get("rightOnly");
        assertEquals(1, rightOnly.size());
        assertEquals("3", rightOnly.get("prop3"));

        HashMap<String, HashMap<String, Object>> different = (HashMap<String, HashMap<String, Object>>) result.get("different");
        assertEquals(1, different.size());
        HashMap<String, Object> pairs = different.get("prop4");
        assertEquals("four", pairs.get("left"));
        assertEquals("for", pairs.get("right"));

        HashMap<String, Object> inCommon = (HashMap<String, Object>) result.get("inCommon");
        assertEquals(1, inCommon.size());
        assertEquals("val1", inCommon.get("prop1"));
    }

    @Test
    public void shouldCompareTwoEqualGraphsByQuery() {
        // given
        initLocalGraph();

        // when
        final String query = "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, m";
        final String boltQuery = "CALL db.indexes() YIELD tokenNames, properties, state, type\n" +
                "WHERE state = 'ONLINE' AND type = 'node_unique_property'\n" +
                "WITH collect({labels: tokenNames, properties: properties, type: type}) AS schema\n" +
                "MATCH (n)\n" +
                "OPTIONAL MATCH (n)-[r]->(m)\n" +
                "WITH collect(n) + collect(m) AS nodes, collect(r) AS relationships, schema\n" +
                "UNWIND nodes AS node\n" +
                "RETURN {nodes: collect(DISTINCT node), relationships: relationships, schema: schema} AS graph\n";
        TestUtil.testResult(db, "CALL apoc.bolt.load($url, $boltQuery, {}, $boltConfig) YIELD row\n" +
                        "CALL apoc.diff.graphs($sourceQuery, row.graph, $diffConfig) YIELD difference, entityType, id, sourceLabel, destLabel, source, dest\n" +
                        "RETURN difference, entityType, id, sourceLabel, destLabel, source, dest",
                map("sourceQuery", query,
                        "boltQuery", boltQuery,
                        "url", neo4jContainer.getBoltUrl(),
                        "boltConfig", map("virtual", true, "withRelationshipNodeProperties", true),
                        "diffConfig", Collections.emptyMap()),
                (r) -> {
                    // then
                    assertFalse(r.hasNext()); // the two graphs are equal
                });
    }

    @Test
    public void shouldCompareTwoDifferentGraphsByQuery() {
        // given
        initLocalGraph();

        // when
        final String query = "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, m";
        final String boltQuery = "CALL db.indexes() YIELD tokenNames, properties, state, type\n" +
                "WHERE state = 'ONLINE' AND type = 'node_unique_property'\n" +
                "WITH collect({labels: tokenNames, properties: properties, type: type}) AS schema\n" +
                "MATCH (n:Person{name: 'Michael Jordan'})\n" +
                "OPTIONAL MATCH (n)-[r]->(m)\n" +
                "WITH collect(n) + collect(m) AS nodes, collect(r) AS relationships, schema\n" +
                "UNWIND nodes AS node\n" +
                "RETURN {nodes: collect(DISTINCT node), relationships: relationships, schema: schema} AS graph\n";
        TestUtil.testResult(db, "CALL apoc.bolt.load($url, $boltQuery, {}, $boltConfig) YIELD row\n" +
                        "CALL apoc.diff.graphs($sourceQuery, row.graph, $diffConfig) YIELD difference, entityType, id, sourceLabel, destLabel, source, dest\n" +
                        "RETURN difference, entityType, id, sourceLabel, destLabel, source, dest",
                map("sourceQuery", query,
                        "boltQuery", boltQuery,
                        "url", neo4jContainer.getBoltUrl(),
                        "boltConfig", map("virtual", true, "withRelationshipNodeProperties", true),
                        "diffConfig", Collections.emptyMap()),
                (r) -> {
                    // then
                    final List<Map<String, Object>> expectedRows = Arrays.asList(
                            map("entityType", "Node", "sourceLabel", null, "difference", "Total count", "id", null, "source", 3L, "dest", 1L, "destLabel", null),
                            map("entityType", "Node", "sourceLabel", null, "difference", "Count by Label", "id", null, "source", map("Person", 3L), "dest", map("Person", 1L), "destLabel", null),
                            map("entityType", "Relationship", "sourceLabel", null, "difference", "Total count", "id", null, "source", 1L, "dest", 0L, "destLabel", null),
                            map("entityType", "Relationship", "sourceLabel", null, "difference", "Count by Type", "id", null, "source", map("KNOWS", 1L), "dest", map(), "destLabel", null),
                            map("entityType", "Node", "sourceLabel", "Person", "difference", "Destination Entity not found", "id", 20L, "source", map("name", "Tom Burton"), "dest", null, "destLabel", null),
                            map("entityType", "Node", "sourceLabel", "Person", "difference", "Destination Entity not found", "id", 21L, "source", map("name", "John William"), "dest", null, "destLabel", null),
                            map("entityType", "Relationship", "sourceLabel", "KNOWS", "difference", "Destination Entity not found", "id", 0L, "source", map("start", map("name", "Tom Burton"),
                                    "end", map("name", "John William"),
                                    "properties", map("time", OffsetTime.parse("12:50:35.556+01:00"), "since", 2016L)
                            ), "dest", null, "destLabel", null)
                    );
                    int index = 0;
                    while (r.hasNext()) {
                        final Map<String, Object> next = r.next();
                        assertEquals(expectedRows.get(index), next);
                        ++index;
                    }
                    assertEquals(index, expectedRows.size());
                });
    }

    @Test
    public void shouldCompareTwoDifferentNodesByQuery() {
        // given
        initLocalGraph();
        db.execute("MERGE (n:Person{name: 'Michael Jordan'}) ON MATCH SET n.age = 55");

        // when
        final String query = "MATCH (n:Person{name: 'Michael Jordan'}) OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, m";
        final String boltQuery = "CALL db.indexes() YIELD tokenNames, properties, state, type\n" +
                "WHERE state = 'ONLINE' AND type = 'node_unique_property'\n" +
                "WITH collect({labels: tokenNames, properties: properties, type: type}) AS schema\n" +
                "MATCH (n:Person{name: 'Michael Jordan'})\n" +
                "OPTIONAL MATCH (n)-[r]->(m)\n" +
                "WITH collect(n) + collect(m) AS nodes, collect(r) AS relationships, schema\n" +
                "UNWIND nodes AS node\n" +
                "RETURN {nodes: collect(DISTINCT node), relationships: relationships, schema: schema} AS graph\n";
        TestUtil.testResult(db, "CALL apoc.bolt.load($url, $boltQuery, {}, $boltConfig) YIELD row\n" +
                        "CALL apoc.diff.graphs($sourceQuery, row.graph, $diffConfig) YIELD difference, entityType, id, sourceLabel, destLabel, source, dest\n" +
                        "RETURN difference, entityType, id, sourceLabel, destLabel, source, dest",
                map("sourceQuery", query,
                        "boltQuery", boltQuery,
                        "url", neo4jContainer.getBoltUrl(),
                        "boltConfig", map("virtual", true, "withRelationshipNodeProperties", true),
                        "diffConfig", Collections.emptyMap()),
                (r) -> {
                    // then
                    final Map<String, Object> expected = map("entityType", "Node", "sourceLabel", "Person", "difference", "Different Properties", "id", 0L, "source", map("age", 55L), "dest", map("age", 54L), "destLabel", "Person");
                    assertTrue(r.hasNext()); // the two nodes have different properties
                    assertEquals(expected, r.next());
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void shouldCompareTwoDifferentPathsByQuery() {
        // given
        initLocalGraph();
        db.execute("MATCH ()-[r:KNOWS]-() SET r.since = 2000");

        // when
        final String query = "MATCH p = ()-[:KNOWS]->() RETURN p";
        final String boltQuery = "CALL db.indexes() YIELD tokenNames, properties, state, type\n" +
                "WHERE state = 'ONLINE' AND type = 'node_unique_property'\n" +
                "WITH collect({labels: tokenNames, properties: properties, type: type}) AS schema\n" +
                "MATCH p = ()-[:KNOWS]->()\n" +
                "RETURN {nodes: nodes(p), relationships: relationships(p), schema: schema} AS graph\n";
        TestUtil.testResult(db, "CALL apoc.bolt.load($url, $boltQuery, {}, $boltConfig) YIELD row\n" +
                        "CALL apoc.diff.graphs($sourceQuery, row.graph, $diffConfig) YIELD difference, entityType, id, sourceLabel, destLabel, source, dest\n" +
                        "RETURN difference, entityType, id, sourceLabel, destLabel, source, dest",
                map("sourceQuery", query,
                        "boltQuery", boltQuery,
                        "url", neo4jContainer.getBoltUrl(),
                        "boltConfig", map("virtual", true, "withRelationshipNodeProperties", true),
                        "diffConfig", Collections.emptyMap()),
                (r) -> {
                    // then
                    final Map<String, Object> expected = map("entityType", "Relationship", "sourceLabel", "KNOWS", "difference", "Destination Entity not found", "id", 0L, "source", map(
                            "start", map("name", "Tom Burton"),
                            "end", map("name", "John William"),
                            "properties", map("time", OffsetTime.parse("12:50:35.556+01:00"), "since", 2000L)
                    ), "dest", null, "destLabel", null);
                    assertTrue(r.hasNext()); // the relationships have different properties
                    final Map<String, Object> next = r.next();
                    assertEquals(expected, next);
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void shouldCompareTwoDifferentPathsByBoltQueryUrl() {
        // given
        initLocalGraph();
        db.execute("MATCH ()-[r:KNOWS]-() SET r.since = 2000");

        // when
        final String localQuery = "MATCH p = ()-[:KNOWS]->() RETURN p";
        final String remoteQuery = "MATCH p = ()-[:KNOWS]->() RETURN p";
        TestUtil.testResult(db, "CALL apoc.diff.graphs($localQuery, $remoteQuery, $diffConfig) YIELD difference, entityType, id, sourceLabel, destLabel, source, dest\n" +
                        "RETURN difference, entityType, id, sourceLabel, destLabel, source, dest",
                map("localQuery", localQuery, "remoteQuery", remoteQuery,
                        "diffConfig", Collections.singletonMap("dest", Util.map("target", Collections.singletonMap("value", neo4jContainer.getBoltUrl())))),
                (r) -> {
                    // then
                    final Map<String, Object> expected = map("entityType", "Relationship", "sourceLabel", "KNOWS", "difference", "Destination Entity not found", "id", 0L, "source", map(
                            "start", map("name", "Tom Burton"),
                            "end", map("name", "John William"),
                            "properties", map("time", OffsetTime.parse("12:50:35.556+01:00"), "since", 2000L)
                    ), "dest", null, "destLabel", null);
                    assertTrue(r.hasNext()); // the relationships have different properties
                    final Map<String, Object> next = r.next();
                    assertEquals(expected, next);
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void shouldInjectQueryParams() {
        // given
        initLocalGraph();
        db.execute("MATCH ()-[r:KNOWS]-() SET r.since = 2000");

        // when
        final String localQuery = "MATCH p = (n:Person{name: $name})-[:KNOWS]->() RETURN p";
        final String remoteQuery = "MATCH p = (n:Person{name: $name})-[:KNOWS]->() RETURN p";
        TestUtil.testResult(db, "CALL apoc.diff.graphs($localQuery, $remoteQuery, $diffConfig) YIELD difference, entityType, id, sourceLabel, destLabel, source, dest\n" +
                        "RETURN difference, entityType, id, sourceLabel, destLabel, source, dest",
                map("localQuery", localQuery, "remoteQuery", remoteQuery,
                        "diffConfig", Util.map("dest", Util.map("target", Collections.singletonMap("value", neo4jContainer.getBoltUrl()),
                                "params", Util.map("name", "Tom Burton")),
                                "source", Util.map("params", Util.map("name", "John William")))),
                (r) -> {
                    // then

                    /*
                    +-----------------------------------------------------------------------------------------------+
                    | difference       | entityType     | id     | sourceLabel | destLabel | source | dest          |
                    +-----------------------------------------------------------------------------------------------+
                    | "Count by Label" | "Node"         | <null> | <null>      | <null>    | {}     | {Person -> 2} |
                    | "Total count"    | "Relationship" | <null> | <null>      | <null>    | 0      | 1             |
                    | "Count by Type"  | "Relationship" | <null> | <null>      | <null>    | {}     | {KNOWS -> 1}  |
                    +-----------------------------------------------------------------------------------------------+
                    3 rows
                     */

                    Map<String, Object> expected = map("entityType", "Node", "sourceLabel", null, "difference", "Total count", "id", null,
                            "source", 0L, "dest", 2L, "destLabel", null);
                    assertTrue(r.hasNext()); // the relationships have different properties
                    Map<String, Object> next = r.next();
                    assertEquals(expected, next);

                    assertTrue(r.hasNext()); // the relationships have different properties
                    expected = map("entityType", "Node", "sourceLabel", null, "difference", "Count by Label", "id", null,
                            "source", Collections.emptyMap(), "dest", Collections.singletonMap("Person", 2L), "destLabel", null);
                    next = r.next();
                    assertEquals(expected, next);

                    assertTrue(r.hasNext()); // the relationships have different properties
                    expected = map("entityType", "Relationship", "sourceLabel", null, "difference", "Total count", "id", null,
                            "source", 0L, "dest", 1L, "destLabel", null);
                    next = r.next();
                    assertEquals(expected, next);

                    assertTrue(r.hasNext()); // the relationships have different properties
                    expected = map("entityType", "Relationship", "sourceLabel", null, "difference", "Count by Type", "id", null,
                            "source", Collections.emptyMap(), "dest", Collections.singletonMap("KNOWS", 1L), "destLabel", null);
                    next = r.next();
                    assertEquals(expected, next);
                    assertFalse(r.hasNext());
                });
    }

    private void initLocalGraph() {
        try (Scanner scanner = new Scanner(Thread
                    .currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream("init_neo4j_diff.cypher"))
                .useDelimiter(";")) {
            while (scanner.hasNext()) {
                String statement = scanner.next().trim();
                if (statement.isEmpty()) {
                    continue;
                }
                db.execute(statement);
            }
        }
    }

}
