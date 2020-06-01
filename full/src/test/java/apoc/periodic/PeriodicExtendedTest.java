package apoc.periodic;

import apoc.load.Jdbc;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class PeriodicExtendedTest {

    public static final long RUNDOWN_COUNT = 1000;
    public static final int BATCH_SIZE = 399;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void initDb() throws Exception {
        TestUtil.registerProcedure(db, Periodic.class, Jdbc.class);
        db.executeTransactionally("call apoc.periodic.list() yield name call apoc.periodic.cancel(name) yield name as name2 return count(*)");
    }

    @Test
    public void testRock_n_roll() throws Exception {
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
    public void testTerminateRockNRoll() throws Exception {
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
    public void testIterateErrors() throws Exception {
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
    public void testIterateJDBC() throws Exception {
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
    public void testRock_n_roll_while() throws Exception {
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
        final String query = "CALL apoc.periodic.rock_n_roll_while('return coalescence($previous, 3) - 1 as loop', " +
                "'match (p:Person) return pp', " +
                "'MATCH (p) where p = $p SET p.lastname = p.name', " +
                "10)";
        try {
            testFail(query);
        } catch (QueryExecutionException e) {
            String expected = "Failed to invoke procedure `apoc.periodic.rock_n_roll_while`: Caused by: java.lang.RuntimeException: Exception for field `cypherLoop`, message: Unknown function 'coalescence' (line 1, column 16 (offset: 15))\n" +
                    "\"EXPLAIN return coalescence($previous, 3) - 1 as loop\"\n" +
                    "                ^\n" +
                    "Exception for field `cypherIterate`, message: Variable `pp` not defined (line 1, column 33 (offset: 32))\n" +
                    "\"EXPLAIN match (p:Person) return pp\"\n" +
                    "                                 ^";
            assertEquals(expected, e.getMessage());
            throw e;
        }
    }

    private void testFail(String query) {
        testCall(db, query, row -> fail("The test should fail but it didn't"));
    }
}
