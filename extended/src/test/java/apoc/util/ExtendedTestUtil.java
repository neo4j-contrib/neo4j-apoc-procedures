package apoc.util;

import apoc.util.collection.Iterables;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.assertion.Assert;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallAssertions;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ExtendedTestUtil {

    /**
     * Mock URLAccessChecker instance with checkURL(URL url) {return url}
     */
    public static class MockURLAccessChecker {
        public static final URLAccessChecker INSTANCE = url -> url;
    }

    public static void assertRelationship(Relationship rel,
                                          String expectedRelType, Map<String, Object> expectedProps,
                                          List<String> expectedStartNodeLabels,
                                          Map<String, Object> expectedStartNodeProps,
                                          List<String> expectedEndNodeLabels,
                                          Map<String, Object> expectedEndNodeProps) {
        
        Node startNode = rel.getStartNode();
        Node endNode = rel.getEndNode();

        assertMapEquals(expectedProps, rel.getAllProperties());
        assertEquals(RelationshipType.withName(expectedRelType), rel.getType());

        Set<Label> expectedStartLabelSet = expectedStartNodeLabels.stream()
                .map(Label::label)
                .collect(Collectors.toSet());
        
        assertEquals(expectedStartLabelSet, Iterables.asSet(startNode.getLabels()));
        assertMapEquals(expectedStartNodeProps, startNode.getAllProperties());

        Set<Label> expectedEndLabelSet = expectedEndNodeLabels.stream()
                .map(Label::label)
                .collect(Collectors.toSet());
        assertEquals(expectedEndLabelSet, Iterables.asSet(endNode.getLabels()));
        assertMapEquals(expectedEndNodeProps, endNode.getAllProperties());
    }

    public static void assertMapEquals(Map<String, Object> expected, Map<String, Object> actual) {
        assertMapEquals(null, expected, actual);
    }
    
    public static void assertMapEquals(String errMsg, Map<String, Object> expected, Map<String, Object> actual) {
        if (expected == null) {
            assertNull(actual);
        } else {
            assertEquals(errMsg, expected.keySet(), actual.keySet());

            actual.forEach((key, value) -> {
                if (value instanceof Map mapVal) {
                    assertMapEquals(errMsg, (Map<String, Object>) expected.get(key), mapVal);
                } else if (value.getClass().isArray() && expected.get(key).getClass().isArray()) {
                    Object[] expectedArray = Iterators.array(expected.get(key));
                    Object[] actualArray = Iterators.array(value);
                    assertArrayEquals(expectedArray, actualArray);
                } else {
                    assertEquals(errMsg, expected.get(key), value);
                }
            });
        }
    }
    
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

    public static void assertFails(GraphDatabaseService db, String query, Map<String,Object> params, String expectedErrMsg) {
        try {
            testCall(db, query, params, r -> fail("Should fail due to " + expectedErrMsg));
        } catch (Exception e) {
            String actualErrMsg = e.getMessage();
            assertTrue("Actual err. message is: " + actualErrMsg, actualErrMsg.contains(expectedErrMsg));
        }
    }
}
