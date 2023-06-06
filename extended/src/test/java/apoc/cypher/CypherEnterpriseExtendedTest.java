package apoc.cypher;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.collection.Iterables;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import static apoc.cypher.CypherTestUtil.CREATE_RESULT_NODES;
import static apoc.cypher.CypherTestUtil.CREATE_RETURNQUERY_NODES;
import static apoc.cypher.CypherTestUtil.SET_AND_RETURN_QUERIES;
import static apoc.cypher.CypherTestUtil.SET_NODE;
import static apoc.cypher.CypherTestUtil.SIMPLE_RETURN_QUERIES;
import static apoc.cypher.CypherTestUtil.assertReturnQueryNode;
import static apoc.cypher.CypherTestUtil.testRunProcedureWithSetAndReturnResults;
import static apoc.cypher.CypherTestUtil.testRunProcedureWithSimpleReturnResults;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.importFolder;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testResult;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class CypherEnterpriseExtendedTest {
    private static final String SET_RETURN_FILE = "set_and_return.cypher";
    private static final String MATCH_RETURN_FILE = "match_and_return.cypher";

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.EXTENDED), true)
                .withNeo4jConfig("dbms.transaction.timeout", "60s");
        neo4jContainer.start();
        session = neo4jContainer.getSession();

        // create cypher files
        createContainerFile(SET_RETURN_FILE, SET_AND_RETURN_QUERIES);
        createContainerFile(MATCH_RETURN_FILE, SIMPLE_RETURN_QUERIES);
    }

    private static void createContainerFile(String fileName, String fileContent) {
        try {
            File file = new File(importFolder, fileName);
            FileUtils.writeStringToFile(file, fileContent, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @After
    public void after() {
        session.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n"));
    }

    @Test
    public void testParallelTransactionGuard() {
        // given
        String parallelQuery = "UNWIND range(0,9) as id CALL apoc.util.sleep(10000) WITH id RETURN id";

        // when
        try {
            int size = 10_000;
            testResult(neo4jContainer.getSession(),
                    "CALL apoc.cypher.parallel2('" + parallelQuery + "', {a: range(1, $size)}, 'a')",
                    map("size", size),
                    r -> {});
        } catch (Exception ignored) {}

        // then
        boolean anyLingeringParallelTx = neo4jContainer.getSession().readTransaction(tx -> {
            var currentTxs = tx.run("SHOW TRANSACTIONS").stream();
            return currentTxs.anyMatch( record -> record.get( "currentQuery" ).toString().contains(parallelQuery));
        });

        assertFalse(anyLingeringParallelTx);
    }

    @Test
    public void testRunFileWithSetAndResults() {
        String query = "CALL apoc.cypher.runFile($file)";
        Map<String, Object> params = Map.of("file", SET_RETURN_FILE);

        testRunProcedureWithSetAndReturnResults(session, query, params);
    }

    @Test
    public void testRunFileWithResults() {
        String query = "CALL apoc.cypher.runFile($file)";
        Map<String, Object> params = Map.of("file", MATCH_RETURN_FILE);

        testRunProcedureWithSimpleReturnResults(session, query, params);
    }

    @Test
    public void testRunFilesWithSetAndResults() {
        String query = "CALL apoc.cypher.runFiles([$file])";
        Map<String, Object> params = Map.of("file", SET_RETURN_FILE);

        testRunProcedureWithSetAndReturnResults(session, query, params);
    }

    @Test
    public void testRunFilesWithResults() {
        String query = "CALL apoc.cypher.runFiles([$file])";
        Map<String, Object> params = Map.of("file", MATCH_RETURN_FILE);

        testRunProcedureWithSimpleReturnResults(session, query, params);
    }

    @Test
    public void testCypherParallelWithSetAndResults() {
        session.writeTransaction(tx -> tx.run(CREATE_RESULT_NODES));

        String query = "CALL apoc.cypher.parallel($file, {a: range(1,4)}, 'a')";
        Map<String, Object> params = Map.of("file", SET_NODE);

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> testCall(session, query, params, (res) -> {})
        );
        String expectedMessage = "Set property for property 'updated' on database 'neo4j' is not allowed for user 'neo4j' with roles [PUBLIC, admin] overridden by READ.";
        Assertions.assertThat(e.getMessage()).contains(expectedMessage);
    }

    @Test
    public void testCypherParallel2WithSetAndResults() {
        session.writeTransaction(tx -> tx.run(CREATE_RESULT_NODES));

        String query = "CALL apoc.cypher.parallel2($file, {a: range(1,4)}, 'a')";
        Map<String, Object> params = Map.of("file", SET_NODE);

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> testCall(session, query, params, (res) -> {})
        );
        String expectedMessage = "Creating new property name on database 'neo4j' is not allowed for user 'neo4j' with roles [PUBLIC, admin] overridden by READ";
        Assertions.assertThat(e.getMessage()).contains(expectedMessage);
    }

    @Test
    public void testCypherParallelWithResults() {
        String query = "CALL apoc.cypher.parallel($file, {a: range(1,3)}, 'a')";
        Map<String, Object> params = Map.of("file", SIMPLE_RETURN_QUERIES);

        testCypherParallelCommon(query, params);
    }

    @Test
    public void testCypherParallel2WithResults() {
        String query = "CALL apoc.cypher.parallel2($file, {a: range(1,3)}, 'a')";
        Map<String, Object> params = Map.of("file", SIMPLE_RETURN_QUERIES);

        testCypherParallelCommon(query, params);
    }

    private void testCypherParallelCommon(String query, Map<String, Object> params) {
        session.writeTransaction(tx -> tx.run(CREATE_RETURNQUERY_NODES));

        testResult(session, query, params, r -> {
            assertBatchCypherParallel(r);
            assertBatchCypherParallel(r);
            assertBatchCypherParallel(r);

            assertFalse(r.hasNext());
        });
    }

    private void assertBatchCypherParallel(Iterator<Map<String, Object>> r) {
        Map<String, Object> next = r.next();
        assertReturnQueryNode(0L, (Map<String, Node>) next.get("value"));
        next = r.next();
        assertReturnQueryNode(1L, (Map<String, Node>) next.get("value"));
        next = r.next();
        assertReturnQueryNode(2L, (Map<String, Node>) next.get("value"));
        next = r.next();
        assertReturnQueryNode(3L, (Map<String, Node>) next.get("value"));
    }

    @Test
    public void testCypherMapParallelWithResults() {
        String query = """
                MATCH (n:ReturnQuery) WITH COLLECT(n) AS list
                CALL apoc.cypher.mapParallel('MATCH (_)-[r:REL]->(o:Other) RETURN r, o', {}, list)
                YIELD value RETURN value""";
        Map<String, Object> params = Map.of("file", SIMPLE_RETURN_QUERIES);

        testCypherMapParallelCommon(query, params);
    }

    @Test
    public void testCypherMapParallel2WithResults() {
        String query = """
                MATCH (n:ReturnQuery) WITH COLLECT(n) AS list
                CALL apoc.cypher.mapParallel2('MATCH (_)-[r:REL]->(o:Other) RETURN r, o', {}, list, 1)
                YIELD value RETURN value""";
        Map<String, Object> params = Map.of("file", SIMPLE_RETURN_QUERIES);

        testCypherMapParallelCommon(query, params);
    }

    private void testCypherMapParallelCommon(String query, Map<String, Object> params) {
        session.writeTransaction(tx -> tx.run(CREATE_RETURNQUERY_NODES));

        testResult(session, query, params, r -> {
            Map<String, Object> next = r.next();
            assertOtherNodeAndRel(0L, (Map<String, Object>) next.get("value"));
            next = r.next();
            assertOtherNodeAndRel(1L, (Map<String, Object>) next.get("value"));
            next = r.next();
            assertOtherNodeAndRel(2L, (Map<String, Object>) next.get("value"));
            next = r.next();
            assertOtherNodeAndRel(3L, (Map<String, Object>) next.get("value"));

            assertFalse(r.hasNext());
        });
    }

    public static void assertOtherNodeAndRel(long id, Map<String, Object> result) {
        Node n = (Node) result.get("o");
        assertEquals(List.of("Other"), Iterables.asList(n.labels()));
        assertEquals(Map.of("idOther", id), n.asMap());

        Relationship rel = (Relationship) result.get("r");
        assertEquals("REL", rel.type());
        assertEquals(Map.of("idRel", id), rel.asMap());
    }
}
