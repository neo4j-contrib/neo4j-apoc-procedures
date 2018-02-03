package apoc.periodic;

import apoc.load.Jdbc;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.sql.SQLException;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class PeriodicTest {

    public static final long RUNDONW_COUNT = 1000;
    public static final int BATCH_SIZE = 399;
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Periodic.class, Jdbc.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testSubmitStatement() throws Exception {
        String callList = "CALL apoc.periodic.list()";
        // force pre-caching the queryplan
        assertFalse(db.execute(callList).hasNext());

        testCall(db, "CALL apoc.periodic.submit('foo','create (:Foo)')",
                (row) -> {
                    assertEquals("foo", row.get("name"));
                    assertEquals(false, row.get("done"));
                    assertEquals(false, row.get("cancelled"));
                    assertEquals(0L, row.get("delay"));
                    assertEquals(0L, row.get("rate"));
                });

        long count = tryReadCount(50, "MATCH (:Foo) RETURN COUNT(*) AS count", 1L);

        assertThat(String.format("Expected %d, got %d ", 1L, count), count, equalTo(1L));

        testCall(db, callList, (r) -> assertEquals(true, r.get("done")));
    }

    @Test
    public void testRunDown() throws Exception {
        db.execute("UNWIND range(1,{count}) AS id CREATE (n:Person {id:id})", MapUtil.map("count", RUNDONW_COUNT)).close();

        String query = "MATCH (p:Person) WHERE NOT p:Processed WITH p LIMIT {limit} SET p:Processed RETURN count(*)";

        testCall(db, "CALL apoc.periodic.commit({query},{params})", MapUtil.map("query", query, "params", MapUtil.map("limit", BATCH_SIZE)), r -> {
            assertEquals((long) Math.ceil((double) RUNDONW_COUNT / BATCH_SIZE), r.get("executions"));
            assertEquals(RUNDONW_COUNT, r.get("updates"));
        });

        ResourceIterator<Long> it = db.execute("MATCH (p:Processed) RETURN COUNT(*) AS c").<Long>columnAs("c");
        long count = it.next();
        it.close();
        assertEquals(RUNDONW_COUNT, count);

    }

    @Test
    public void testRock_n_roll() throws Exception {
        // setup
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        // when&then

        // TODO: remove forcing rule based in the 2nd statement next line when 3.0.2 is released, due to https://github.com/neo4j/neo4j/pull/7152
        testResult(db, "CALL apoc.periodic.rock_n_roll('match (p:Person) return p', 'WITH {p} as p SET p.lastname =p.name REMOVE p.name', 10)", result -> {
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
    public void testIterateErrors() throws Exception {
        testResult(db, "CALL apoc.periodic.rock_n_roll('UNWIND range(0,99) as id RETURN id', 'CREATE (:Foo {id: 1 / ({id}%10)})', 10)", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
            assertEquals(0L, row.get("committedOperations"));
            assertEquals(10L, row.get("failedOperations"));
            assertEquals(10L, row.get("failedBatches"));
            Map<String, Object> batchErrors = map("org.neo4j.graphdb.TransactionFailureException: Transaction was marked as successful, but unable to commit transaction so rolled back.", 10L);
            assertEquals(batchErrors, ((Map) row.get("batch")).get("errors"));
            Map<String, Object> operationsErrors = map("/ by zero", 10L);
            assertEquals(operationsErrors, ((Map) row.get("operations")).get("errors"));
        });
    }

    @Test
    public void testPeriodicIterateErrors() throws Exception {
        testResult(db, "CALL apoc.periodic.iterate('UNWIND range(0,99) as id RETURN id', 'CREATE null', {batchSize:10,iterateList:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
            assertEquals(0L, row.get("committedOperations"));
            assertEquals(100L, row.get("failedOperations"));
            assertEquals(10L, row.get("failedBatches"));
            Map<String, Object> batchErrors = map("org.neo4j.graphdb.TransactionFailureException: Transaction was marked as successful, but unable to commit transaction so rolled back.", 10L);
            assertEquals(batchErrors, ((Map) row.get("batch")).get("errors"));
            Map<String, Object> operationsErrors = map("Parentheses are required to identify nodes in patterns, i.e. (null) (line 1, column 56 (offset: 55))\n" +
                    "\"UNWIND {_batch} AS _batch WITH _batch.id AS id  CREATE null\"\n" +
                    "                                                        ^", 10L);
            assertEquals(operationsErrors, ((Map) row.get("operations")).get("errors"));
        });
    }

    @Test
    public void testIteratePrefixGiven() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'WITH {p} as p SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIterate() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIteratePrefix() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIterateBatch() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname = p.name REMOVE p.name', {batchSize:10, iterateList:true, parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            System.out.println(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIterateBatchPrefix() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname = p.name REMOVE p.name', {batchSize:10, iterateList:true, parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            System.out.println(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIterateRetries() throws Exception {
        testResult(db, "CALL apoc.periodic.iterate('return 1', 'CREATE (n {prop: 1/{_retry}})', {retries:1})", result -> {
            Map<String, Object> row = Iterators.single(result);
            System.out.println(result);
            assertEquals(1L, row.get("batches"));
            assertEquals(1L, row.get("total"));
            assertEquals(1L, row.get("retries"));
        });
    }

    @Test
    public void testIterateFail() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();
        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'WITH {p} as p SET p.lastname = p.name REMOVE x.name', {batchSize:10,parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            System.out.println("row = " + row);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
            assertEquals(100L, row.get("failedOperations"));
            assertEquals(0L, row.get("committedOperations"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(0L, row.get("count"))
        );
    }

    @Test
    public void testIterateJDBC() throws Exception {
        TestUtil.ignoreException(() -> {
            testResult(db, "CALL apoc.periodic.iterate('call apoc.load.jdbc(\"jdbc:mysql://localhost:3306/northwind?user=root\",\"customers\")', 'create (c:Customer) SET c += {row}', {batchSize:10,parallel:true})", result -> {
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
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        // when&then
        testResult(db, "CALL apoc.periodic.rock_n_roll_while('return coalesce({previous},3)-1 as loop', 'match (p:Person) return p', 'MATCH (p) where p={p} SET p.lastname =p.name', 10)", result -> {
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

    @Test
    public void testCountdown() {
        int startValue = 10;
        int rate = 1;

        db.execute("CREATE (counter:Counter {c: " + startValue + "})");
        String statementToRepeat = "MATCH (counter:Counter) SET counter.c = counter.c - 1 RETURN counter.c as count";

        Map<String, Object> params = map("statement", statementToRepeat, "rate", rate);
        testResult(db, "CALL apoc.periodic.countdown('decrement',{statement}, {rate})", params, r -> {
            try {
                // Number of iterations per rate (in seconds)
                Thread.sleep(startValue * rate * 1000);
            } catch (InterruptedException e) {

            }

            Map<String, Object> result = db.execute("MATCH (counter:Counter) RETURN counter.c as c").next();
            assertEquals(0L, result.get("c"));
        });
    }


    private long tryReadCount(int maxAttempts, String statement, long expected) throws InterruptedException {
        int attempts = 0;
        long count;
        do {
            Thread.sleep(100);
            attempts++;
            count = readCount(statement);
        } while (attempts < maxAttempts && count != expected);
        return count;
    }

    private long readCount(String statement) {
        try (ResourceIterator<Long> it = db.execute(statement).columnAs("count")) {
            return Iterators.single(it);
        }
    }
}
