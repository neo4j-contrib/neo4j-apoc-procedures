package apoc.ml;

import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

import static apoc.ml.MLUtil.ERROR_NULL_INPUT;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MLTestUtil {
    public static void assertNullInputFails(GraphDatabaseService db, String query, Map<String, Object> params) {
        try {
            testCall(db, query, params,
                    (row) -> fail("Should fail due to null input")
            );
        } catch (RuntimeException e) {
            String message = e.getMessage();
            assertTrue("Current error message is: " + message, 
                    message.contains(ERROR_NULL_INPUT)
            );
        }
    }
}
