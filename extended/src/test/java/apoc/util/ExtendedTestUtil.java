package apoc.util;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.test.assertion.Assert;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static apoc.util.ExtendedTestContainerUtil.singleResultFirstColumn;
import static apoc.util.TestUtil.testCallAssertions;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ExtendedTestUtil {

    /**
     * similar to @link {@link TestUtil#testCallEventually(GraphDatabaseService, String, Consumer, long)}
     * but re-execute the {@link GraphDatabaseService#executeTransactionally(String, Map, ResultTransformer)} in case of error
     * instead of interrupting and throwing it
     */
    public static void testRetryCallEventually(GraphDatabaseService db, String call, Map<String,Object> params, Consumer<Map<String, Object>> consumer, long timeout) {
        Assert.assertEventually(() -> {
            try {
                return db.executeTransactionally(call, params, r -> {
                    testCallAssertions(r, consumer);
                    return true;
                });
            } catch (Exception e) {
                return false;
            }
        }, (v) -> v, timeout, TimeUnit.SECONDS);
    }

    public static void testRetryCallEventually(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer, long timeout) {
        testRetryCallEventually(db, call, Collections.emptyMap(), consumer, timeout);
    }

    /**
     * similar to @link {@link ExtendedTestUtil#testRetryCallEventually(GraphDatabaseService, String, Consumer, long)}
     * but with multiple results
     */
    public static void testResultEventually(GraphDatabaseService db, String call, Consumer<Result> resultConsumer, long timeout) {
        assertEventually(() -> {
            try {
                return db.executeTransactionally(call, Map.of(), r -> {
                    resultConsumer.accept(r);
                    return true;
                });
            } catch (Exception e) {
                return false;
            }
        }, (v) -> v, timeout, TimeUnit.SECONDS);
    }
}
