package apoc.diff;

import apoc.bolt.Bolt;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.OffsetTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiffFullTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static Neo4jContainerExtension neo4jContainer;

    @BeforeClass
    public static void setup() throws Exception {
        neo4jContainer = createEnterpriseDB(true)
                .withInitScript("init_neo4j_diff.cypher")
                .withLogging()
                .withoutAuthentication();
        neo4jContainer.start();

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, Bolt.class, DiffFull.class);
    }

    @AfterClass
    public static void tearDown() {
        neo4jContainer.close();
    }

    @Before
    public void before() throws Exception {
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
                db.executeTransactionally(statement);
            }
        }
    }

    @After
    public void after() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }
    
    @Test
    public void shouldCompareTwoEqualGraphsByQuery() {
        // when
        final String query = "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, m";
        final String boltQuery = "CALL db.indexes() YIELD labelsOrTypes, properties, state, uniqueness\n" +
                "WHERE state = 'ONLINE' AND uniqueness = 'UNIQUE'\n" +
                "WITH collect({labels: labelsOrTypes, properties: properties, type: uniqueness}) AS schema\n" +
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
        // when
        final String query = "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, m";
        final String boltQuery = "CALL db.indexes() YIELD labelsOrTypes, properties, state, uniqueness\n" +
                "WHERE state = 'ONLINE' AND uniqueness = 'UNIQUE'\n" +
                "WITH collect({labels: labelsOrTypes, properties: properties, type: uniqueness}) AS schema\n" +
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
                    final List<Map<String, Object>> expectedRows = List.of(
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
                    final List<Map<String, Object>> actuals = r.stream().collect(Collectors.toList());
                    assertEquals(actuals.size(), expectedRows.size());

                    IntStream.range(0, actuals.size()).forEach(index -> {
                        final Map<String, Object> actual = actuals.get(index);
                        getMapAssertions(expectedRows.get(index), actual);
                    });
                });
    }

    @Test
    public void shouldCompareTwoDifferentNodesByQuery() {
        db.executeTransactionally("MERGE (n:Person{name: 'Michael Jordan'}) ON MATCH SET n.age = 55");

        // when
        final String query = "MATCH (n:Person{name: 'Michael Jordan'}) OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, m";
        final String boltQuery = "CALL db.indexes() YIELD labelsOrTypes, properties, state, uniqueness\n" +
                "WHERE state = 'ONLINE' AND uniqueness = 'UNIQUE'\n" +
                "WITH collect({labels: labelsOrTypes, properties: properties, type: uniqueness}) AS schema\n" +
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
                    getMapAssertions(expected, r.next());
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void shouldCompareTwoDifferentPathsByQuery() {
        db.executeTransactionally("MATCH ()-[r:KNOWS]-() SET r.since = 2000");

        // when
        final String query = "MATCH p = ()-[:KNOWS]->() RETURN p";
        final String boltQuery = "CALL db.indexes() YIELD labelsOrTypes, properties, state, uniqueness\n" +
                "WHERE state = 'ONLINE' AND uniqueness = 'UNIQUE'\n" +
                "WITH collect({labels: labelsOrTypes, properties: properties, type: uniqueness}) AS schema\n" +
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
                    getMapAssertions(expected, next);
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void shouldCompareTwoDifferentPathsByBoltQueryUrl() {
        db.executeTransactionally("MATCH ()-[r:KNOWS]-() SET r.since = 2000");

        // when
        final String localQuery = "MATCH p = ()-[:KNOWS]->() RETURN p";
        final String remoteQuery = "MATCH p = ()-[:KNOWS]->() RETURN p";
        TestUtil.testResult(db, "CALL apoc.diff.graphs($localQuery, $remoteQuery, $diffConfig) YIELD difference, entityType, id, sourceLabel, destLabel, source, dest\n" +
                        "RETURN difference, entityType, id, sourceLabel, destLabel, source, dest",
                map("localQuery", localQuery, "remoteQuery", remoteQuery,
                        "diffConfig", Collections.singletonMap("dest", map("target", Collections.singletonMap("value", neo4jContainer.getBoltUrl())))),
                (r) -> {
                    // then
                    final Map<String, Object> expected = map("entityType", "Relationship", "sourceLabel", "KNOWS", "difference", "Destination Entity not found", "id", 0L, "source", map(
                            "start", map("name", "Tom Burton"),
                            "end", map("name", "John William"),
                            "properties", map("time", OffsetTime.parse("12:50:35.556+01:00"), "since", 2000L)
                    ), "dest", null, "destLabel", null);
                    assertTrue(r.hasNext()); // the relationships have different properties
                    final Map<String, Object> next = r.next();
                    getMapAssertions(expected, next);
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void shouldInjectQueryParams() {
        db.executeTransactionally("MATCH ()-[r:KNOWS]-() SET r.since = 2000");

        // when
        final String localQuery = "MATCH p = (n:Person{name: $name})-[:KNOWS]->() RETURN p";
        final String remoteQuery = "MATCH p = (n:Person{name: $name})-[:KNOWS]->() RETURN p";
        TestUtil.testResult(db, "CALL apoc.diff.graphs($localQuery, $remoteQuery, $diffConfig) YIELD difference, entityType, id, sourceLabel, destLabel, source, dest\n" +
                        "RETURN difference, entityType, id, sourceLabel, destLabel, source, dest",
                map("localQuery", localQuery, "remoteQuery", remoteQuery,
                        "diffConfig", map("dest", map("target", Collections.singletonMap("value", neo4jContainer.getBoltUrl()),
                                "params", map("name", "Tom Burton")),
                                "source", map("params", map("name", "John William")))),
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
                    getMapAssertions(expected, next);

                    assertTrue(r.hasNext()); // the relationships have different properties
                    expected = map("entityType", "Node", "sourceLabel", null, "difference", "Count by Label", "id", null,
                            "source", Collections.emptyMap(), "dest", Collections.singletonMap("Person", 2L), "destLabel", null);
                    next = r.next();
                    getMapAssertions(expected, next);

                    assertTrue(r.hasNext()); // the relationships have different properties
                    expected = map("entityType", "Relationship", "sourceLabel", null, "difference", "Total count", "id", null,
                            "source", 0L, "dest", 1L, "destLabel", null);
                    next = r.next();
                    getMapAssertions(expected, next);

                    assertTrue(r.hasNext()); // the relationships have different properties
                    expected = map("entityType", "Relationship", "sourceLabel", null, "difference", "Count by Type", "id", null,
                            "source", Collections.emptyMap(), "dest", Collections.singletonMap("KNOWS", 1L), "destLabel", null);
                    next = r.next();
                    getMapAssertions(expected, next);
                    assertFalse(r.hasNext());
                });
    }

    private void getMapAssertions(Map<String, Object> expected, Map<String, Object> next) {
        expected.forEach((k, v) -> {
            if (k.equals("id")) {
                assertTrue(v == null 
                        ? next.get(k) == null 
                        : next.get(k) instanceof Long);
            } else {
                assertEquals(v, next.get(k));
            }
        });
    }
}
