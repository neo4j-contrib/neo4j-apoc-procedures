package apoc.sequence;

import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Created by Valentino Maiorca on 6/29/17.
 */
public class SequenceTest {

    private static GraphDatabaseService db;

    private static String CREATE_SEQUENCE_QUERY = "CALL apoc.sequence.create($sequence) YIELD node as n return n";

    private static String NEXT_VALUE_QUERY = "CALL apoc.sequence.next($sequence) YIELD value as n return n";

    private static String NEXT_VALUES_QUERY = "CALL apoc.sequence.next($sequence, $length) YIELD value as n return n";


    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Sequence.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testDefaultSequence() throws Throwable {
        final String NEW_SEQUENCE = "TEST1_SEQUENCE";

        // Test creation
        TestUtil.testCall(db, CREATE_SEQUENCE_QUERY, MapUtil.map("sequence", NEW_SEQUENCE), (row) -> {
        });
        TestUtil.testCall(db, String.format("MATCH (n:%s {%s: $sequence}) RETURN count(*) AS result",
                Sequence.SEQUENCE_LABEL.name(),
                Sequence.NAME_PROPERTY),
                MapUtil.map("sequence", NEW_SEQUENCE),
                (row) -> assertEquals(1L, row.get("result")));

        //Test increment
        for (long tries = 0; tries < 100; tries++) {
            long finalTries = tries;
            TestUtil.testCall(db, NEXT_VALUE_QUERY, MapUtil.map("sequence", NEW_SEQUENCE),
                    (row) -> assertEquals(finalTries, row.get("n")));
        }
    }

    @Test
    public void testSequenceMultithread() throws Throwable {
        Map<String, Object> params = MapUtil.map("sequence", "TEST2_SEQUENCE");

        TestUtil.testCall(db, CREATE_SEQUENCE_QUERY, params, (row) -> {
        });

        long threadsNumber = 1000;
        ExecutorService executorService = Executors.newFixedThreadPool(50);

        for (int threadNumber = 0; threadNumber < threadsNumber; threadNumber++)
            executorService.submit(() -> TestUtil.testCall(db, NEXT_VALUE_QUERY, params, row -> {
            }));

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);

        TestUtil.testCall(db, NEXT_VALUE_QUERY, params, row -> assertEquals(threadsNumber, row.get("n")));
    }

    @Test
    public void testMultipleSequencesMultithread() throws Throwable {
        long threadsNumber = 1000;
        String[] names = new String[5];
        for (int i = 0; i < 5; i++)
            names[i] = "Seq_" + i;

        for (String name : names)
            TestUtil.testCall(db, CREATE_SEQUENCE_QUERY, MapUtil.map("sequence", name), (row) -> {
            });

        ExecutorService executorService = Executors.newFixedThreadPool(50);
        for (int i = 0; i < threadsNumber; i++)
            for (String name : names)
                executorService.submit(() ->
                        TestUtil.testCall(db, NEXT_VALUE_QUERY, MapUtil.map("sequence", name), row -> {
                        }));

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);

        for (String name : names)
            TestUtil.testCall(db, NEXT_VALUE_QUERY, MapUtil.map("sequence", name),
                    (row) -> assertEquals(threadsNumber, row.get("n")));
    }

    @Test
    public void testMultipleSequencesMultithread2() throws Throwable {
        int sequenceNumber = 100;
        String[] names = new String[sequenceNumber];
        for (int i = 0; i < sequenceNumber; i++)
            names[i] = "Seq2_" + i;

        ExecutorService executorService = Executors.newFixedThreadPool(50);
        for (String name : names) {
            TestUtil.testCall(db, CREATE_SEQUENCE_QUERY, MapUtil.map("sequence", name), row -> {
            });
            for (int i = 0; i < 6; i++)
                executorService.submit(() ->
                        TestUtil.testCall(db, NEXT_VALUE_QUERY, MapUtil.map("sequence", name), row -> {
                        }));
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);

        for (String name : names)
            TestUtil.testCall(db, NEXT_VALUE_QUERY, MapUtil.map("sequence", name),
                    (row) -> assertEquals(6L, row.get("n")));
    }

    @Test
    public void testGetSequence() throws Throwable {
        Map<String, Object> params = MapUtil.map("sequence", "TestSequenceRoot");
        TestUtil.testCall(db, String.format("MATCH (n:%s {%s: $sequence}) RETURN count(*) AS result",
                Sequence.SEQUENCE_LABEL.name(),
                Sequence.NAME_PROPERTY),
                params,
                (row) -> assertEquals(0L, row.get("result")));

        TestUtil.testCall(db, CREATE_SEQUENCE_QUERY, params, row -> {
        });

        TestUtil.testCall(db, String.format("MATCH (n:%s {%s: $sequence}) RETURN count(*) AS result",
                Sequence.SEQUENCE_LABEL.name(),
                Sequence.NAME_PROPERTY),
                params,
                (row) -> assertEquals(1L, row.get("result")));
    }

    @Test
    public void testRegisterAndUseNewProvider() throws Throwable {
        SequenceProvider<String> provider = new SequenceProvider<String>() {
            @Override
            public String start() {
                return "a";
            }

            @Override
            public String next(String previous) {
                return previous + "a";
            }

            @Override
            public String getName() {
                return "StupidStringProvider";
            }
        };
        Sequence.registerProvider(provider);

        Map<String, Object> params = MapUtil.map("sequence", "StupidStringSequence");

        TestUtil.testCall(db, "CALL apoc.sequence.create($sequence, 'StupidStringProvider') YIELD node as n return n",
                params, row -> {
                });
        TestUtil.testCall(db, NEXT_VALUE_QUERY, params, (row) -> assertEquals("a", row.get("n")));
        TestUtil.testCall(db, NEXT_VALUE_QUERY, params, (row) -> assertEquals("aa", row.get("n")));
        TestUtil.testCall(db, NEXT_VALUE_QUERY, params, (row) -> assertEquals("aaa", row.get("n")));
    }

    @Test
    public void testNextWithLength() throws Throwable {
        Map<String, Object> params = new HashMap<>();
        params.put("sequence", "TestLength");
        TestUtil.testCall(db, CREATE_SEQUENCE_QUERY, params, (row) -> {
        });

        params.put("length", 100);

        TestUtil.testCallCount(db, NEXT_VALUES_QUERY, params, 100);
    }

}
