package apoc.kernel;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class KernelTestUtils {
    public static void checkStatusDetails(GraphDatabaseService db, String query, Predicate<String> statusDetailsCheck) {
        checkStatusDetails(db, query, statusDetailsCheck, Collections.emptyMap(), null);
    }

    public static void checkStatusDetails(GraphDatabaseService db, String query, Predicate<String> statusDetailsCheck, Map<String, Object> params) {
        checkStatusDetails(db, query, statusDetailsCheck, params, null);
    }
    public static void checkStatusDetails(GraphDatabaseService db, String query, Predicate<String> statusDetailsCheck, Map<String, Object> params, String startQuery) {
        String transactionQuery = startQuery == null ? query : startQuery;

        final Future<?> future = showStatusDetailsAsync(db, transactionQuery, statusDetailsCheck);

        db.executeTransactionally(query, params, Result::resultAsString);

        try {
            future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Future<?> showStatusDetailsAsync(GraphDatabaseService db,
                                                         String transactionQuery,
                                                         Predicate<String> statusDetailsCheck) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        return executor.submit(() -> {

            assertEventually(() -> db.executeTransactionally("SHOW TRANSACTIONS YIELD currentQuery, statusDetails " +
                            "WHERE currentQuery STARTS WITH $query " +
                            "RETURN statusDetails",
                    Map.of("query", transactionQuery),
                    result -> {
                        final ResourceIterator<String> iterator = result.columnAs("statusDetails");
                        if (!iterator.hasNext()) {
                            return false;
                        }
                        String row = iterator.next();
                        System.out.println("row = " + row);
                        assertTrue("Actual `statusDetails` is: " + row, statusDetailsCheck.test(row));

                        return !iterator.hasNext();
                    }), (value) -> value, 10L, TimeUnit.SECONDS);
        });
    }

}
