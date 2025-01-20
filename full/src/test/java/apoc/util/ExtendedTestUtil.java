package apoc.util;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallAssertions;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.assertion.Assert;

public class ExtendedTestUtil {

    public static void assertMapEquals(Map<String, Object> expected, Map<String, Object> actual) {
        assertMapEquals(null, expected, actual);
    }

    public static void assertMapEquals(String errMsg, Map<String, Object> expected, Map<String, Object> actual) {
        if (expected == null) {
            assertNull(actual);
        } else {
            assertEquals(errMsg, expected.keySet(), actual.keySet());

            actual.forEach((key, actualValue) -> {
                Object expectedValue = expected.get(key);
                boolean valuesAreArrays = key != null
                        && actualValue != null
                        && actualValue.getClass().isArray()
                        && expectedValue.getClass().isArray();

                if (actualValue instanceof Map) {
                    assertMapEquals(errMsg, (Map<String, Object>) expectedValue, (Map<String, Object>) actualValue);
                } else if (valuesAreArrays) {
                    Object[] expectedArray = Iterators.array(expectedValue);
                    Object[] actualArray = Iterators.array(actualValue);
                    assertArrayEquals(expectedArray, actualArray);
                } else {
                    assertEquals(errMsg, expectedValue, actualValue);
                }
            });
        }
    }

    public static void assertRelationship(
            Relationship rel,
            String expectedRelType,
            Map<String, Object> expectedProps,
            List<String> expectedStartNodeLabels,
            Map<String, Object> expectedStartNodeProps,
            List<String> expectedEndNodeLabels,
            Map<String, Object> expectedEndNodeProps) {

        Node startNode = rel.getStartNode();
        Node endNode = rel.getEndNode();
        assertMapEquals(expectedProps, rel.getAllProperties());
        assertEquals(RelationshipType.withName(expectedRelType), rel.getType());
        Set<Label> expectedStartLabelSet =
                expectedStartNodeLabels.stream().map(Label::label).collect(Collectors.toSet());

        assertEquals(expectedStartLabelSet, Iterables.asSet(startNode.getLabels()));
        assertMapEquals(expectedStartNodeProps, startNode.getAllProperties());
        Set<Label> expectedEndLabelSet =
                expectedEndNodeLabels.stream().map(Label::label).collect(Collectors.toSet());
        assertEquals(expectedEndLabelSet, Iterables.asSet(endNode.getLabels()));
        assertMapEquals(expectedEndNodeProps, endNode.getAllProperties());
    }

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
