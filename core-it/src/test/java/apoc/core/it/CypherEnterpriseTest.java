package apoc.core.it;

import apoc.util.Neo4jContainerExtension;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;

import java.util.List;
import java.util.Map;

import static apoc.cypher.CypherTestUtil.CREATE_RESULT_NODES;
import static apoc.cypher.CypherTestUtil.CREATE_RETURNQUERY_NODES;
import static apoc.cypher.CypherTestUtil.SET_AND_RETURN_QUERIES;
import static apoc.cypher.CypherTestUtil.SET_NODE;
import static apoc.cypher.CypherTestUtil.SIMPLE_RETURN_QUERIES;
import static apoc.cypher.CypherTestUtil.assertResultNode;
import static apoc.cypher.CypherTestUtil.assertReturnQueryNode;
import static apoc.cypher.CypherTestUtil.testRunProcedureWithSetAndReturnResults;
import static apoc.cypher.CypherTestUtil.testRunProcedureWithSimpleReturnResults;
import static apoc.util.TestContainerUtil.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class CypherEnterpriseTest {
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.CORE), true);
        neo4jContainer.start();
        session = neo4jContainer.getSession();
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
    public void testRunManyWithSetAndResults() {
        String query = "CALL apoc.cypher.runMany($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_AND_RETURN_QUERIES);

        testRunProcedureWithSetAndReturnResults(session, query, params);
    }

    @Test
    public void testRunManyWithResults() {
        String query = "CALL apoc.cypher.runMany($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunProcedureWithSimpleReturnResults(session, query, params);
    }

    @Test
    public void testRunManyReadOnlyWithSetAndResults() {
        String query = "CALL apoc.cypher.runManyReadOnly($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_AND_RETURN_QUERIES);

        session.writeTransaction(tx -> tx.run(CREATE_RESULT_NODES));

        // even if this procedure is read-only and execute a write operation, it doesn't fail but just skip the statements
        testCallEmpty(session, query, params);
    }

    @Test
    public void testRunManyReadOnlyWithResults() {
        String query = "CALL apoc.cypher.runManyReadOnly($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunProcedureWithSimpleReturnResults(session, query, params);
    }

    @Test
    public void testRunWriteWithSetAndResults() {
        String query = "CALL apoc.cypher.runWrite($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_NODE);

        testRunSingleStatementProcedureWithSetAndResults(query, params);
    }

    @Test
    public void testRunWriteWithResults() {
        String query = "CALL apoc.cypher.runWrite($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunSingleStatementProcedureWithResults(query, params);
    }

    @Test
    public void testDoItWithSetAndResults() {
        String query = "CALL apoc.cypher.doIt($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_NODE);

        testRunSingleStatementProcedureWithSetAndResults(query, params);
    }

    @Test
    public void testDoItWithResults() {
        String query = "CALL apoc.cypher.doIt($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunSingleStatementProcedureWithResults(query, params);
    }

    @Test
    public void testRunWithSetAndResults() {
        String query = "CALL apoc.cypher.run($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_NODE);

        session.writeTransaction(tx -> tx.run(CREATE_RESULT_NODES));

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> testCall(session, query, params, (res) -> {})
        );
        String expectedMessage = "Set property for property 'updated' on database 'neo4j' is not allowed for user 'neo4j' with roles [PUBLIC, admin] overridden by READ.";
        Assertions.assertThat(e.getMessage()).contains(expectedMessage);
    }

    @Test
    public void testRunWithResults() {
        String query = "CALL apoc.cypher.run($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunSingleStatementProcedureWithResults(query, params);
    }

    private static void testRunSingleStatementProcedureWithResults(String query, Map<String, Object> params) {
        session.writeTransaction(tx -> tx.run(CREATE_RETURNQUERY_NODES));

        testResult(session, query, params, r -> {
            Map<String, Object> next = r.next();
            assertReturnQueryNode(0L, (Map<String, Node>) next.get("value"));
            next = r.next();
            assertReturnQueryNode(1L, (Map<String, Node>) next.get("value"));
            next = r.next();
            assertReturnQueryNode(2L, (Map<String, Node>) next.get("value"));
            next = r.next();
            assertReturnQueryNode(3L, (Map<String, Node>) next.get("value"));

            assertFalse(r.hasNext());
        });
    }

    private static void testRunSingleStatementProcedureWithSetAndResults(String query, Map<String, Object> params) {
        session.writeTransaction(tx -> tx.run(CREATE_RESULT_NODES));

        testResult(session, query, params, r -> {
            Map<String, Object> next = r.next();
            assertResultNode(0L, (Map<String, Node>) next.get("value"));
            next = r.next();
            assertResultNode(1L, (Map<String, Node>) next.get("value"));
            next = r.next();
            assertResultNode(2L, (Map<String, Node>) next.get("value"));
            next = r.next();
            assertResultNode(3L, (Map<String, Node>) next.get("value"));

            assertFalse(r.hasNext());
        });
    }

}