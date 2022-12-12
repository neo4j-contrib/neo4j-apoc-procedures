package apoc.periodic;

import apoc.create.Create;
import apoc.load.Jdbc;
import apoc.nlp.gcp.GCPProcedures;
import apoc.nodes.NodesExtended;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PeriodicExtendedTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void initDb() {
        TestUtil.registerProcedure(db, Periodic.class, NodesExtended.class, GCPProcedures.class, Create.class, PeriodicExtended.class, Jdbc.class);
    }
    
    @Test
    public void testRebindWithNlpWriteProcedure() {
        // use case: https://community.neo4j.com/t5/neo4j-graph-platform/use-of-apoc-periodic-iterate-with-apoc-nlp-gcp-classify-graph/m-p/56846#M33854
        final String iterate = "MATCH (node:Article) RETURN node";
        final String action = "CALL apoc.nlp.gcp.classify.graph(node, $nlpConf) YIELD graph RETURN null";
        testRebindCommon(iterate, action, 0, this::assertNodeDeletedErr);
        
        final String actionRebind = "CALL apoc.nlp.gcp.classify.graph(apoc.node.rebind(node), $nlpConf) YIELD graph RETURN null";
        testRebindCommon(iterate, actionRebind, 2, this::assertNoErrors);
        
        // "manual" rebind, i.e. "return id(node) as id" in iterate query, and "match .. where id(n)=id" in action query
        final String iterateId = "MATCH (node:Article) RETURN id(node) AS id";
        final String actionId = "MATCH (node) WHERE id(node) = id CALL apoc.nlp.gcp.classify.graph(node, $nlpConf) YIELD graph RETURN null";

        testRebindCommon(iterateId, actionId, 2, this::assertNoErrors);
    }
    
    @Test
    public void testRebindWithMapIterationAndCreateRelationshipProcedure() {
        final String iterate = "MATCH (art:Article) RETURN {key: art, key2: 'another'} as map";
        final String action = "CREATE (node:Category) with map.key as art, node call apoc.create.relationship(art, 'CATEGORY', {b: 1}, node) yield rel return rel";
        testRebindCommon(iterate, action, 0, this::assertNodeDeletedErr);
        
        final String actionRebind = "WITH apoc.any.rebind(map) AS map " + action;
        testRebindCommon(iterate, actionRebind, 1, this::assertNoErrors);
    }

    private void assertNoErrors(Map<String, Object> r) {
        assertEquals(Collections.emptyMap(), r.get("errorMessages"));
    }

    private void assertNodeDeletedErr(Map<String, Object> r) {
        assertTrue(((Map<String, Object>) r.get("errorMessages"))
                .keySet().stream()
                .anyMatch(k -> k.matches("Node\\[\\d+] is deleted and cannot be used to create a relationship")));
    }

    private void testRebindCommon(String iterate, String action, int expected, Consumer<Map<String, Object>> assertions) {
        final Map<String, Object> nlpConf = map("key", "myKey", "nodeProperty", "content", "write", true, "unsupportedDummyClient", true);
        final Map<String, Object> config = map("params", map("nlpConf", nlpConf));
        
        db.executeTransactionally("CREATE (:Article {content: 'contentBody'})");
        testCall(db,"CALL apoc.periodic.iterate($iterate, $action, $config)", 
                map( "iterate" , iterate, "action", action, "config", config),
                assertions);

        testCallCount(db, "MATCH p=(:Category)<-[:CATEGORY]-(:Article) RETURN p", expected);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testRock_n_roll() {
        // setup
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");

        // when&then

        testResult(db, "CALL apoc.periodic.rock_n_roll('match (p:Person) return p', 'WITH $p as p SET p.lastname =p.name REMOVE p.name', 10)", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });
        // then
        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testTerminateRockNRoll() {
        PeriodicTestUtils.testTerminatePeriodicQuery(db, "CALL apoc.periodic.rock_n_roll('UNWIND range(0,1000) as id RETURN id', 'CREATE (:Foo {id: $id})', 10)");
    }

    public void testTerminatePeriodicQuery(String periodicQuery) {
        killPeriodicQueryAsync();
        try {
            testResult(db, periodicQuery, result -> {
                Map<String, Object> row = Iterators.single(result);
                assertEquals( periodicQuery + " result: " + row.toString(), true, row.get("wasTerminated"));
            });
            fail("Should have terminated");
        } catch(Exception tfe) {
            assertEquals(tfe.getMessage(),true, tfe.getMessage().contains("terminated"));
        }
    }

    public void killPeriodicQueryAsync() {
        new Thread(() -> {
            int retries = 10;
            try {
                while (retries-- > 0 && !terminateQuery("apoc.periodic")) {
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                // ignore
            }
        }).start();
    }

    boolean terminateQuery(String pattern) {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        KernelTransactions kernelTransactions = dependencyResolver.resolveDependency(KernelTransactions.class);
        long numberOfKilledTransactions = kernelTransactions.activeTransactions().stream()
                .filter(kernelTransactionHandle ->
                        kernelTransactionHandle.executingQuery().map(query -> query.rawQueryText().contains(pattern))
                                .orElse(false)
                )
                .map(kernelTransactionHandle -> kernelTransactionHandle.markForTermination(Status.Transaction.Terminated))
                .count();
        return numberOfKilledTransactions > 0;
    }

    @Test
    public void testIterateErrors() {
        testResult(db, "CALL apoc.periodic.rock_n_roll('UNWIND range(0,99) as id RETURN id', 'CREATE (:Foo {id: 1 / ($id % 10)})', 10)", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
            assertEquals(0L, row.get("committedOperations"));
            assertEquals(100L, row.get("failedOperations"));
            assertEquals(10L, row.get("failedBatches"));
            Map<String, Object> batchErrors = map("org.neo4j.graphdb.QueryExecutionException: / by zero", 10L);
            assertEquals(batchErrors, ((Map) row.get("batch")).get("errors"));
            Map<String, Object> operationsErrors = map("/ by zero", 10L);
            assertEquals(operationsErrors, ((Map) row.get("operations")).get("errors"));
        });
    }


    @Test
    public void testIterateJDBC() {
        TestUtil.ignoreException(() -> {
            testResult(db, "CALL apoc.periodic.iterate('call apoc.load.jdbc(\"jdbc:mysql://localhost:3306/northwind?user=root\",\"customers\")', 'create (c:Customer) SET c += $row', {batchSize:10,parallel:true})", result -> {
                Map<String, Object> row = Iterators.single(result);
                assertEquals(3L, row.get("batches"));
                assertEquals(29L, row.get("total"));
            });

            testCall(db,
                    "MATCH (p:Customer) return count(p) as count",
                    row -> assertEquals(29L, row.get("count"))
            );
        }, SQLException.class);
    }

    @Test
    public void testRock_n_roll_while() {
        // setup
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");

        // when&then
        testResult(db, "CALL apoc.periodic.rock_n_roll_while('return coalesce($previous,3)-1 as loop', 'match (p:Person) return p', 'MATCH (p) where p=$p SET p.lastname =p.name', 10)", result -> {
            long l = 0;
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                assertEquals(2L - l, row.get("loop"));
                assertEquals(10L, row.get("batches"));
                assertEquals(100L, row.get("total"));
                l += 1;
            }
            assertEquals(2L, l);
        });
        // then
        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }


    @Test(expected = QueryExecutionException.class)
    public void testRockNRollWhileLoopFail() {
        final String query = "CALL apoc.periodic.rock_n_roll_while('return coalescence($previous, 3) - 1 as loop', " +
                "'match (p:Person) return p', " +
                "'MATCH (p) where p = {p} SET p.lastname = p.name', " +
                "10)";
        testFail(query);
    }

    @Test(expected = QueryExecutionException.class)
    public void testRockNRollWhileIterateFail() {
        final String query = "CALL apoc.periodic.rock_n_roll_while('return coalesce($previous, 3) - 1 as loop', " +
                "'match (p:Person) return pp', " +
                "'MATCH (p) where p = {p} SET p.lastname = p.name', " +
                "10)";
        testFail(query);
    }


    @Test(expected = QueryExecutionException.class)
    public void testRockNRollIterateFail() {
        final String query = "CALL apoc.periodic.rock_n_roll('match (pp:Person) return p', " +
                "'WITH $p as p SET p.lastname = p.name REMOVE p.name', " +
                "10)";
        testFail(query);
    }

    @Test(expected = QueryExecutionException.class)
    public void testRockNRollActionFail() {
        final String query = "CALL apoc.periodic.rock_n_roll('match (p:Person) return p', " +
                "'WITH $p as p SET pp.lastname = p.name REMOVE p.name', " +
                "10)";
        testFail(query);
    }

    @Test(expected = QueryExecutionException.class)
    public void testRockNRollWhileFail() {
        final String newline = System.lineSeparator();
        final String query = "CALL apoc.periodic.rock_n_roll_while('return coalescence($previous, 3) - 1 as loop', " +
                "'match (p:Person) return pp', " +
                "'MATCH (p) where p = $p SET p.lastname = p.name', " +
                "10)";
        try {
            testFail(query);
        } catch (QueryExecutionException e) {
            String expected = "Failed to invoke procedure `apoc.periodic.rock_n_roll_while`: Caused by: java.lang.RuntimeException: Exception for field `cypherLoop`, message: Unknown function 'coalescence' (line 1, column 16 (offset: 15))" + newline +
                    "\"EXPLAIN return coalescence($previous, 3) - 1 as loop\"" + newline +
                    "                ^\n" + // XXX: do not replace this newline (\n) with the native newline value
                    "Exception for field `cypherIterate`, message: Variable `pp` not defined (line 1, column 33 (offset: 32))" + newline +
                    "\"EXPLAIN match (p:Person) return pp\"" + newline +
                    "                                 ^";
            assertEquals(expected, e.getMessage());
            throw e;
        }
    }

    private void testFail(String query) {
        testCall(db, query, row -> fail("The test should fail but it didn't"));
    }
}
