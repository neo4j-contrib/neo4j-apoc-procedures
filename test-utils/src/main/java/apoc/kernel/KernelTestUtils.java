package apoc.kernel;

import apoc.util.TestUtil;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.neo4j.test.assertion.Assert.assertEventually;

public class KernelTestUtils {

    public static void checkStatusDetails(GraphDatabaseService db, String query, Map<String, Object> params) {
        checkStatusDetails(db, query, params, null);
    }
    public static void checkStatusDetails(GraphDatabaseService db, String query, Map<String, Object> params, String startQuery) {
        String finalStartQuery = startQuery == null ? query : startQuery;
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<String> future = executor.submit(() -> db.executeTransactionally(query, params, Result::resultAsString));

        assertEventually(() -> TestUtil.<String>singleResultFirstColumn(db,
                "SHOW TRANSACTIONS YIELD statusDetails, currentQuery where currentQuery STARTS WITH $startQuery RETURN statusDetails", 
                Map.of("startQuery", finalStartQuery)),
                StringUtils::isNotEmpty, 
                20L, TimeUnit.SECONDS);
        
        try {
            future.get();
        } catch (Exception e) {
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e);
        }
    }
}
