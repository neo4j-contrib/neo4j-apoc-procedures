package apoc.custom;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;

import java.io.IOException;
import java.util.Map;

import static apoc.ExtendedApocConfig.APOC_KAFKA_ENABLED;
import static apoc.custom.CypherProceduresHandler.*;
import static apoc.util.DbmsTestUtil.startDbWithApocConfigs;
import static apoc.util.SystemDbTestUtil.PROCEDURE_DEFAULT_REFRESH;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CypherProcedureTestUtil {
    public final static String QUERY_CREATE = "RETURN $input1 + $input2 as answer";
    public static DatabaseManagementService startDbWithCustomApocConfigs(TemporaryFolder storeDir) throws IOException {
        return startDbWithApocConfigs(storeDir,
                Map.of(
                        CUSTOM_PROCEDURES_REFRESH, PROCEDURE_DEFAULT_REFRESH,
                        APOC_KAFKA_ENABLED, "true"
                )
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