package apoc.algo;

import apoc.util.TestUtil;
import org.junit.*;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CentralityTest {
    private static GraphDatabaseService db;
    public static final String RANDOM_GRAPH =
            "FOREACH (_ IN range(0,100) | CREATE ()) " +
                    "WITH 0.1 AS p " +
                    "MATCH (n1),(n2) WITH n1,n2 LIMIT 1000 WHERE rand() < p " +
                    "CREATE (n1)-[:TYPE]->(n2)";

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Algo.class);
        db.execute(RANDOM_GRAPH).close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    // ==========================================================================================

    @Test
    public void shouldHandleEmptyNodeSetWhenUsingBetweenness() {
        assertExpectedResult(0, "CALL apoc.algo.betweenness(['TYPE'],[],'BOTH')" + "");
    }

    @Test
    public void shouldHandleEmptyNodeSetWhenUsingCloseness() {
        assertExpectedResult(0, "CALL apoc.algo.closeness(['TYPE'],[],'BOTH')" + "");
    }

    // ==========================================================================================

    @Test
    public void shouldHandleEmptyRelationshipTypeWhenUsingBetweenness() {
        assertExpectedResult(50, algoQuery("CALL apoc.algo.betweenness([],nodes,'BOTH')"));
    }

    @Test
    public void shouldHandleEmptyRelationshipTypeUsingCloseness() {
        assertExpectedResult(50, algoQuery("CALL apoc.algo.closeness([],nodes,'BOTH')"));
    }

    // ==========================================================================================

    @Test
    public void shouldProvideSameResultUsingEmptyRelationshipTypeOrSpecifyAllTypesWhenUsingBetweenness() {
        assertResultsAreEqual(algoQuery("CALL apoc.algo.betweenness([],nodes,'BOTH')"),
                              algoQuery("CALL apoc.algo.betweenness(['TYPE'],nodes,'BOTH')"));
    }

    @Test
    public void shouldProvideSameResultUsingEmptyRelationshipTypeOrSpecifyAllTypesWhenUsingCloseness() {
        assertResultsAreEqual(algoQuery("CALL apoc.algo.closeness([],nodes,'BOTH')"),
                              algoQuery("CALL apoc.algo.closeness(['TYPE'],nodes,'BOTH')"));
    }


    // ==========================================================================================

    @Test
    public void shouldHandleRelationshipTypesThatDoesNotExistWhenUsingBetweenness() {
        String algo = "CALL apoc.algo.betweenness(['BAD_VALUE'],nodes,'BOTH')";
        String query = algoQuery(algo);
        assertExpectedResult(50, query, 0);
    }

    @Test
    public void shouldHandleRelationshipTypesThatDoesNotExistWhenUsingCentrality() {
        assertExpectedResult(0, algoQuery("CALL apoc.algo.closeness(['BAD_VALUE'],nodes,'BOTH')"));
    }

    // ==========================================================================================

    @Test(expected = RuntimeException.class)
    public void shouldHandleNullNodesWhenUsingBetweenness() {
        assertExpectedResult(0, algoQuery("CALL apoc.algo.betweenness([],null,'BOTH')"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldHandleNullNodesWhenUsingCentrality() {
        assertExpectedResult(0, algoQuery("CALL apoc.algo.closeness([],null,'BOTH')"));
    }

    // ==========================================================================================

    @Test(expected = RuntimeException.class)
    public void shouldHandleNullRelationshipTypesWhenUsingBetweenness() {
        assertExpectedResult(0, algoQuery("CALL apoc.algo.betweenness(null,nodes,'BOTH')"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldHandleNullRelationshipTypesWhenUsingCentrality() {
        assertExpectedResult(0, algoQuery("CALL apoc.algo.closeness(null,nodes,'BOTH')"));
    }

    // ==========================================================================================

    @Test(expected = RuntimeException.class)
    public void shouldHandleNullDirectionWhenUsingBetweenness() {
        assertExpectedResult(0, algoQuery("CALL apoc.algo.betweenness(null,nodes,'null')"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldHandleNullDirectionWhenUsingCentrality() {
        assertExpectedResult(0, algoQuery("CALL apoc.algo.closeness(null,nodes,'null')"));
    }

    // ==========================================================================================

    @Test
    public void shouldReturnExpectedResultCountWhenUsingBetweennessAllDirections() {
        assertExpectedResult(50, algoQuery("CALL apoc.algo.betweenness(['TYPE'],nodes,'OUTGOING')"));

        assertExpectedResult(50, algoQuery("CALL apoc.algo.betweenness(['TYPE'],nodes,'INCOMING')"));

        assertExpectedResult(50, algoQuery("CALL apoc.algo.betweenness(['TYPE'],nodes,'BOTH')"));
    }

    @Test
    public void shouldReturnExpectedResultCountWhenUsingClosenessOutgoingAllDirections() {
        assertExpectedResult(50, algoQuery("CALL apoc.algo.closeness(['TYPE'],nodes,'OUTGOING')"));

        assertExpectedResult(50, algoQuery("CALL apoc.algo.closeness(['TYPE'],nodes,'INCOMING')"));

        assertExpectedResult(50, algoQuery("CALL apoc.algo.closeness(['TYPE'],nodes,'BOTH')"));
    }

// ==========================================================================================

    @Test(expected = RuntimeException.class)
    public void shouldHandleInvalidDirectionWhenUsingBetweenness() {
        assertExpectedResult(0, algoQuery("CALL apoc.algo.betweenness(null,nodes,'INVAlid')"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldHandleInvalidDirectionWhenUsingCentrality() {
        assertExpectedResult(0, algoQuery("CALL apoc.algo.closeness(null,nodes,' ')"));
    }

    // ==========================================================================================

    @Test
    public void shouldBeCaseInsensitiveForDirectionBetweenness() {
        assertExpectedResult(50, algoQuery("CALL apoc.algo.betweenness(['TYPE'],nodes,'InCominG')"));

        assertExpectedResult(50, algoQuery("CALL apoc.algo.betweenness(['TYPE'],nodes,'ouTGoiNg')"));
    }

    @Test
    public void shouldBeCaseInsensitiveForDirectionCloseness() {
        assertExpectedResult(50, algoQuery("CALL apoc.algo.closeness(['TYPE'],nodes,'InCominG')"));

        assertExpectedResult(50, algoQuery("CALL apoc.algo.closeness(['TYPE'],nodes,'ouTGoiNg')"));
    }

    // ==========================================================================================

    public String algoQuery(String algo) {
        return "MATCH (n) WITH n LIMIT 50 " +
                "WITH collect(n) AS nodes " +
                algo + " YIELD node, centrality " +
                "RETURN node, centrality " +
                "ORDER BY centrality DESC";
    }

    private void assertResultsAreEqual(String query1, String query2) {
        try (Transaction ignore = db.beginTx()) {
            Result result1 = db.execute(query1);
            Result result2 = db.execute(query2);
            while (result1.hasNext()) {
                assertTrue(result2.hasNext());
                assertThat(result1.next(), equalTo(result2.next()));
            }
            assertFalse(result2.hasNext());
        }
    }

    private void assertExpectedResult(int expectedResultCount, String query) {
        TestUtil.testResult(db, query, (result) -> {
            for (int i = 0; i < expectedResultCount; i++) {
                assertThat(result.next().get("node"), is(instanceOf(Node.class)));
            }
        });
    }

    private void assertExpectedResult(int expectedResultCount, String query, double expectedCentralityValue) {
        TestUtil.testResult(db, query, (result) -> {
            for (int i = 0; i < expectedResultCount; i++) {
                Map<String, Object> r = result.next();
                assertThat(r.get("node"), is(instanceOf(Node.class)));
                assertThat(r.get("centrality"), equalTo(expectedCentralityValue));
            }
        });
    }
}

