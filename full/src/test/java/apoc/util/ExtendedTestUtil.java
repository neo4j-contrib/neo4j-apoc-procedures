package apoc.util;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallAssertions;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.test.assertion.Assert;

public class ExtendedTestUtil {

    /**
     * similar to @link {@link TestUtil#testCallEventually(GraphDatabaseService, String, Consumer, long)}
     * but re-execute the {@link GraphDatabaseService#executeTransactionally(String, Map, ResultTransformer)} in case of error
     * instead of interrupting and throwing it
     */
    public static void testRetryCallEventually(
            GraphDatabaseService db,
            String call,
            Map<String, Object> params,
            Consumer<Map<String, Object>> consumer,
            long timeout) {
        Assert.assertEventually(
                () -> {
                    try {
                        return db.executeTransactionally(call, params, r -> {
                            testCallAssertions(r, consumer);
                            return true;
                        });
                    } catch (Exception e) {
                        return false;
                    }
                },
                (v) -> v,
                timeout,
                TimeUnit.SECONDS);
    }

    public static void testRetryCallEventually(
            GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer, long timeout) {
        testRetryCallEventually(db, call, Collections.emptyMap(), consumer, timeout);
    }

    /**
     * similar to @link {@link ExtendedTestUtil#testRetryCallEventually(GraphDatabaseService, String, Consumer, long)}
     * but with multiple results
     */
    public static void testResultEventually(
            GraphDatabaseService db, String call, Consumer<Result> resultConsumer, long timeout) {
        testResultEventually(db, call, Collections.emptyMap(), resultConsumer, timeout);
    }

    public static void testResultEventually(
            GraphDatabaseService db,
            String call,
            Map<String, Object> params,
            Consumer<Result> resultConsumer,
            long timeout) {
        assertEventually(
                () -> {
                    try {
                        return db.executeTransactionally(call, params, r -> {
                            resultConsumer.accept(r);
                            return true;
                        });
                    } catch (Exception e) {
                        return false;
                    }
                },
                (v) -> v,
                timeout,
                TimeUnit.SECONDS);
    }

    public static void assertFails(
            GraphDatabaseService db, String query, Map<String, Object> params, String expectedErrMsg) {
        try {
            testCall(db, query, params, r -> fail("Should fail due to " + expectedErrMsg));
        } catch (Exception e) {
            String actualErrMsg = e.getMessage();
            assertTrue("Actual err. message is: " + actualErrMsg, actualErrMsg.contains(expectedErrMsg));
        }
    }

    public static String getLogFileContent() {
        try {
            File logFile = new File(FileUtils.getLogDirectory(), "debug.log");
            return Files.readString(logFile.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
