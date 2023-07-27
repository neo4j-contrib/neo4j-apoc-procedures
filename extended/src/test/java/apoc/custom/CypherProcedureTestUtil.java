package apoc.custom;

import apoc.util.ExtendedTestUtil;
import apoc.util.SystemDbTestUtil;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static apoc.custom.CypherProceduresHandler.*;
import static apoc.util.DbmsTestUtil.startDbWithApocConfigs;
import static apoc.util.ExtendedTestUtil.testRetryCallEventually;
import static apoc.util.SystemDbTestUtil.PROCEDURE_DEFAULT_REFRESH;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class CypherProcedureTestUtil {
    public final static String QUERY_CREATE = "RETURN $input1 + $input2 as answer";
    public static DatabaseManagementService startDbWithCustomApocConfigs(TemporaryFolder storeDir) throws IOException {
        return startDbWithApocConfigs(storeDir,
                Map.of(CUSTOM_PROCEDURES_REFRESH, PROCEDURE_DEFAULT_REFRESH)
        );
    }

    public static void assertProcedureFails(GraphDatabaseService db, String expectedMessage, String query, Map<String, Object> params) {
        try {
            testCall(db, query, params, row -> fail("The test should fail because of: " + expectedMessage));
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            String message = except.getMessage();
            assertTrue("Actual error is: " + message, message.contains(expectedMessage));
        }
    }

    public static void assertProcedureFails(GraphDatabaseService db, String expectedMessage, String query) {
        assertProcedureFails(db, expectedMessage, query, Map.of());
    }
}