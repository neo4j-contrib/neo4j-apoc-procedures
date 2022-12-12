package apoc.diff;

import apoc.create.Create;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import apoc.bolt.Bolt;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.OffsetTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.diff.DiffExtended.COUNT_BY_LABEL;
import static apoc.diff.DiffExtended.COUNT_BY_TYPE;
import static apoc.diff.DiffExtended.DESTINATION_ENTITY_NOT_FOUND;
import static apoc.diff.DiffExtended.DIFFERENT_LABELS;
import static apoc.diff.DiffExtended.DIFFERENT_PROPS;
import static apoc.diff.DiffExtended.NODE;
import static apoc.diff.DiffExtended.RELATIONSHIP;
import static apoc.diff.DiffExtended.TOTAL_COUNT;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class DiffExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;
    private static final String secondDb = "secondDb";
    
    @BeforeClass
    public static void setup() {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED), true)
                .withInitScript("init_neo4j_diff.cypher")
                .withLogging()
                .withoutAuthentication();
        neo4jContainer.start();
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);

        Driver driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.none());
        session = neo4jContainer.getSession();

        try (Session session = driver.session()) {
            session.writeTransaction(tx -> tx.run(String.format("CREATE DATABASE %s;", secondDb)));
        }
        try (Session session = driver.session(SessionConfig.forDatabase(secondDb))) {
            session.writeTransaction(tx -> tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE;"));
            session.writeTransaction(tx -> tx.run("CREATE (m:Person:Other {name: 'Michael Jordan', age: 54}), \n" +
                    "(q:Person {name: 'Jerry Burton', age: 23}), \n" +
                    "(p:Person {name: 'Jack William', age: 22}), \n" +
                    "(q)-[:KNOWS{since:1999, time:time('125035.556+0100')}]->(p);"));
        }
        
        TestUtil.registerProcedure(db, Bolt.class, DiffExtended.class, Create.class);

        try (Transaction tx = db.beginTx()) {
            Node node1Start = tx.createNode(Label.label("Node1Start"));
            Node node1End = tx.createNode(Label.label("Node1End"));
            Relationship rel1 = node1Start.createRelationshipTo(node1End, RelationshipType.withName("REL1"));
            rel1.setProperty("prop1", "val1");
            rel1.setProperty("prop2", 2L);

            Node node2Start = tx.createNode(Label.label("Node2Start"));
            Node node2End = tx.createNode(Label.label("Node2End"));
            Relationship rel2 = node2Start.createRelationshipTo(node2End, RelationshipType.withName("REL2"));
            rel2.setProperty("prop1", "val1");
            rel2.setProperty("prop2", 2L);
            rel2.setProperty("prop4", "four");

            Node node3Start = tx.createNode(Label.label("Node3Start"));
            Node node3End = tx.createNode(Label.label("Node3End"));
            Relationship rel3 = node3Start.createRelationshipTo(node3End, RelationshipType.withName("REL3"));
            rel3.setProperty("prop1", "val1");
            rel3.setProperty("prop3", "3");
            rel3.setProperty("prop4", "for");
            tx.commit();
        }
    }

    @AfterClass
    public static void teardown() {
        neo4jContainer.close();
        db.shutdown();
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
    public void relationshipsWithList() {
        final List<String> list = List.of("tomatoes", "bread", "cookies");
        TestUtil.testCall(
                db,
                "CREATE ()-[rel1:REL $propRel1]->(), ()-[rel2:REL $propRel2]->()\n"
                + "RETURN apoc.diff.relationships(rel1, rel2) AS result",
                Map.of(
                        "propRel1",
                        Map.of("name", "Charlie", "alpha", "one", "born", 1999, "grocery_list", list),
                        "propRel2",
                        Map.of("name", "Hannah", "beta", "two", "born", 1999, "grocery_list", list)),
                r -> {
                    Map<String, Map<String, Object>> res = (Map<String, Map<String, Object>>) r.get("result");
                    Map<String, Object> inCommon = res.get("inCommon");
                    assertArrayEquals(list.toArray(), (String[]) inCommon.get("grocery_list"));
                    assertEquals(1999, inCommon.get("born"));
                    assertEquals(Map.of("alpha", "one"), res.get("leftOnly"));
                    assertEquals(Map.of("beta", "two"), res.get("rightOnly"));
                    assertEquals(Map.of("name", Map.of("left", "Charlie", "right", "Hannah")), res.get("different"));
                });
    }

    @Test
    public void relationshipsSame() {
        String query = "MATCH (node:Node1Start)-[rel]->() RETURN apoc.diff.relationships(rel, rel) as diff";
        commonAssertionSameRels(query);
    }

    @Test
    public void relationshipsDiffering() {
        String query = "MATCH (leftNode:Node2Start)-[rel2]->(), (rightNode:Node3Start)-[rel3]->() " +
                       "RETURN apoc.diff.relationships(rel2, rel3) as diff";
        commonAssertionDifferentRels(query);
    }

    @Test
    public void shouldBeDiffWithVirtualRelationships() {
        String query = """
                MATCH (start1:Node1Start)-[rel1]->(end1), (start2:Node2Start)-[rel2]->(end2)
                WITH apoc.create.vRelationship(start1, type(rel1), {prop1: 'val1', prop2: 2, prop4: 'four'}, end1) AS relA,
                    apoc.create.vRelationship(start2, type(rel2), {prop1: 'val1', prop3: '3', prop4: 'for'}, end2) AS relB
                RETURN apoc.diff.relationships(relA, relB) as diff""";
        commonAssertionDifferentRels(query);
    }

    @Test
    public void shouldBeSameWithVirtualRelationships() {
        String query = "MATCH (start:Node1Start)-[rel]->(end)" +
                       "WITH apoc.create.vRelationship(start, type(rel), {prop1: 'val1', prop2: 2}, end) AS rel "
                       + "RETURN apoc.diff.relationships(rel, rel) as diff";
        commonAssertionSameRels(query);
    }

    private void commonAssertionDifferentRels(String query) {
        Map<String, Object> result = db.executeTransactionally(
                query,
                Map.of(),
                r -> Iterators.single(r.columnAs("diff")));
        assertNotNull(result);

        HashMap<String, Object> leftOnly = (HashMap<String, Object>) result.get("leftOnly");
        assertEquals(1, leftOnly.size());
        assertEquals(2L, leftOnly.get("prop2"));

        HashMap<String, Object> rightOnly = (HashMap<String, Object>) result.get("rightOnly");
        assertEquals(1, rightOnly.size());
        assertEquals("3", rightOnly.get("prop3"));

        HashMap<String, HashMap<String, Object>> different =
                (HashMap<String, HashMap<String, Object>>) result.get("different");
        assertEquals(1, different.size());
        HashMap<String, Object> pairs = different.get("prop4");
        assertEquals("four", pairs.get("left"));
        assertEquals("for", pairs.get("right"));

        HashMap<String, Object> inCommon = (HashMap<String, Object>) result.get("inCommon");
        assertEquals(1, inCommon.size());
        assertEquals("val1", inCommon.get("prop1"));
    }

    private void commonAssertionSameRels(String query) {
        Map<String, Object> result =
                db.executeTransactionally(query, Map.of(), r -> Iterators.single(r.columnAs("diff")));
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
    public void shouldNotFindDifferencesInTheSameDbUsingDatabaseTypeAndFindById() {
        TestUtil.testCallEmpty(db, "CALL apoc.diff.graphs($querySourceDest, $querySourceDest, $conf)",
                map("querySourceDest", "MATCH p = (start)-[rel:KNOWS]->(end) RETURN start, rel, end",
                        "conf", map("dest", map("target", map("type", SourceDestConfig.SourceDestConfigType.DATABASE.name(), "value", "neo4j")),
                                "findById", true
                        )));
    }

    @Test
    public void shouldFindDifferencesUsingDatabaseTypeAndFindById() {
        TestContainerUtil.testResult(session, "CALL apoc.diff.graphs($querySourceDest, $querySourceDest, $conf)",
                map("querySourceDest", "MATCH p = ()-[:KNOWS]->() RETURN p",
                        "conf", map("dest", map("target", map("type", SourceDestConfig.SourceDestConfigType.DATABASE.name(), "value", secondDb)),
                                "findById", true
                        )),
                this::secondDbAssertions);
    }

    @Test
    public void shouldNotFindDifferencesInTheSameDataset() {
        TestUtil.testCallEmpty(db, "CALL apoc.diff.graphs($querySourceDest, $querySourceDest, $conf)",
                map("querySourceDest", "MATCH p = ()-[:KNOWS]->() RETURN p",
                        "conf", map("dest", map("target", map("type", SourceDestConfig.SourceDestConfigType.URL.name(), "value", neo4jContainer.getBoltUrl())))));
    }

    @Test
    public void shouldFindDifferencesInASecondDbUsingUrlConfig() {
        TestUtil.testResult(db, "CALL apoc.diff.graphs($querySourceDest, $querySourceDest, $conf)",
                map("querySourceDest", "MATCH p = ()-[:KNOWS]->() RETURN p",
                        "conf", map("boltConfig", map("databaseName", secondDb),
                                "dest", map("target", map("type", SourceDestConfig.SourceDestConfigType.URL.name(), "value", neo4jContainer.getBoltUrl()))
                        )),
                this::secondDbAssertions);
    }


    private void secondDbAssertions(Iterator<Map<String, Object>> r) {
        Map<String, Object> row = r.next();
        assertEquals(NODE, row.get("entityType"));
        assertEquals("Person", row.get("sourceLabel"));
        assertEquals(DESTINATION_ENTITY_NOT_FOUND, row.get("difference"));
        assertEquals(map("name", "Tom Burton"), row.get("source"));
        assertNull(row.get("dest"));
        assertTrue(row.get("id") instanceof Long);

        row = r.next();
        assertEquals(NODE, row.get("entityType"));
        assertEquals("Person", row.get("sourceLabel"));
        assertEquals(DESTINATION_ENTITY_NOT_FOUND, row.get("difference"));
        assertEquals(map("name", "John William"), row.get("source"));
        assertNull(row.get("dest"));
        assertTrue(row.get("id") instanceof Long);

        row = r.next();
        assertEquals(RELATIONSHIP, row.get("entityType"));
        assertEquals("KNOWS", row.get("sourceLabel"));
        assertEquals(DESTINATION_ENTITY_NOT_FOUND, row.get("difference"));
        final Map<String, Object> sourceRel = map("start", map("name", "Tom Burton"),
                "end", map("name", "John William"),
                "properties", map("time", OffsetTime.parse("12:50:35.556+01:00"), "since", 2016L));
        assertEquals(sourceRel, row.get("source"));
        assertNull(row.get("dest"));
        assertTrue(row.get("id") instanceof Long);
        assertFalse(r.hasNext());
    }

    @Test
    public void shouldFindLabelDifferences() {
        TestUtil.testResult(db, "CALL apoc.diff.graphs($querySourceDest, $querySourceDest, $conf)",
                map("querySourceDest", "MATCH (node:Person {name: 'Michael Jordan'}) RETURN node",
                        "conf", map("boltConfig", map("databaseName", secondDb),
                                "dest", map("target", map("type", SourceDestConfig.SourceDestConfigType.URL.name(), "value", neo4jContainer.getBoltUrl()))
                        )),
                r -> {
                    Map<String, Object> row = r.next();
                    final Map<String, Object> expectedTotalCont = map("entityType", NODE, "sourceLabel", null, "difference", TOTAL_COUNT, "id", null, "source", 1L, "dest", 2L, "destLabel", null);
                    assertEquals(expectedTotalCont, row);
                    row = r.next();
                    final Map<String, Object> expectedCountLabel = map("entityType", NODE, "sourceLabel", null,
                            "difference", COUNT_BY_LABEL,
                            "id", null,
                            "source", map("Person", 1L),
                            "dest", map("Person", 1L, "Other", 1L),
                            "destLabel", null);
                    assertEquals(expectedCountLabel, row);
                    row = r.next();
                    assertEquals(NODE, row.get("entityType"));
                    assertEquals(DIFFERENT_LABELS, row.get("difference"));
                    assertEquals("Person", row.get("sourceLabel"));
                    assertEquals("Person", row.get("destLabel"));
                    assertEquals(List.of("Person"), row.get("source"));
                    assertEquals(List.of("Other", "Person"), row.get("dest"));
                    assertTrue(row.get("id") instanceof Long);
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void shouldFindDifferencesInTheSameDb() {
        db.executeTransactionally("MATCH (n:Person {name: 'Michael Jordan'}) SET n:Other");

        TestUtil.testResult(db, "CALL apoc.diff.graphs($querySource, $queryDest, {})",
                map("querySource", "MATCH (node:Other) RETURN node",
                        "queryDest", "MATCH (node:Person) RETURN node"),
                r -> {
                    Map<String, Object> row = r.next();
                    final Map<String, Object> expectedTotCount = map("entityType", NODE, "sourceLabel", null, "difference", TOTAL_COUNT, "id", null, "source", 2L, "dest", 4L, "destLabel", null);
                    assertEquals(expectedTotCount, row);
                    row = r.next();
                    final Map<String, Object> expectedCountLabel = map("entityType", NODE, "sourceLabel", null, "difference", COUNT_BY_LABEL, "id", null,
                            "source", map("Person", 1L, "Other", 1L),
                            "dest", map("Person", 3L, "Other", 1L), "destLabel", null);
                    assertEquals(expectedCountLabel, row);
                    assertFalse(r.hasNext());
                });

        db.executeTransactionally("MATCH (n:Person:Other) REMOVE n:Other");
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
                        "diffConfig", Collections.emptyMap()
                ),
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
                            map("entityType", NODE, "sourceLabel", null, "difference", TOTAL_COUNT, "id", null, "source", 3L, "dest", 1L, "destLabel", null),
                            map("entityType", NODE, "sourceLabel", null, "difference", COUNT_BY_LABEL, "id", null, "source", map("Person", 3L), "dest", map("Person", 1L), "destLabel", null),
                            map("entityType", RELATIONSHIP, "sourceLabel", null, "difference", TOTAL_COUNT, "id", null, "source", 1L, "dest", 0L, "destLabel", null),
                            map("entityType", RELATIONSHIP, "sourceLabel", null, "difference", COUNT_BY_TYPE, "id", null, "source", map("KNOWS", 1L), "dest", map(), "destLabel", null),
                            map("entityType", NODE, "sourceLabel", "Person", "difference", DESTINATION_ENTITY_NOT_FOUND, "id", 20L, "source", map("name", "Tom Burton"), "dest", null, "destLabel", null),
                            map("entityType", NODE, "sourceLabel", "Person", "difference", DESTINATION_ENTITY_NOT_FOUND, "id", 21L, "source", map("name", "John William"), "dest", null, "destLabel", null),
                            map("entityType", RELATIONSHIP, "sourceLabel", "KNOWS", "difference", DESTINATION_ENTITY_NOT_FOUND, "id", 0L, "source", map("start", map("name", "Tom Burton"),
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
                    final Map<String, Object> expected = map("entityType", NODE, "sourceLabel", "Person", "difference", DIFFERENT_PROPS, "id", 0L, "source", map("age", 55L), "dest", map("age", 54L), "destLabel", "Person");
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
                    final Map<String, Object> expected = map("entityType", RELATIONSHIP, "sourceLabel", "KNOWS", "difference", DESTINATION_ENTITY_NOT_FOUND, "id", 0L, "source", map(
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
                    final Map<String, Object> expected = map("entityType", RELATIONSHIP, "sourceLabel", "KNOWS", "difference", DESTINATION_ENTITY_NOT_FOUND, "id", 0L, "source", map(
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

                    Map<String, Object> expected = map("entityType", NODE, "sourceLabel", null, "difference", TOTAL_COUNT, "id", null,
                            "source", 0L, "dest", 2L, "destLabel", null);
                    assertTrue(r.hasNext()); // the relationships have different properties
                    Map<String, Object> next = r.next();
                    getMapAssertions(expected, next);

                    assertTrue(r.hasNext()); // the relationships have different properties
                    expected = map("entityType", NODE, "sourceLabel", null, "difference", COUNT_BY_LABEL, "id", null,
                            "source", Collections.emptyMap(), "dest", Collections.singletonMap("Person", 2L), "destLabel", null);
                    next = r.next();
                    getMapAssertions(expected, next);

                    assertTrue(r.hasNext()); // the relationships have different properties
                    expected = map("entityType", RELATIONSHIP, "sourceLabel", null, "difference", TOTAL_COUNT, "id", null,
                            "source", 0L, "dest", 1L, "destLabel", null);
                    next = r.next();
                    getMapAssertions(expected, next);

                    assertTrue(r.hasNext()); // the relationships have different properties
                    expected = map("entityType", RELATIONSHIP, "sourceLabel", null, "difference", COUNT_BY_TYPE, "id", null,
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
