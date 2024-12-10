package apoc.ml;

import apoc.util.ExtendedTestUtil;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

import static apoc.ml.MLUtil.*;

public class MLTestUtil {
    
    public static void assertNullInputFails(GraphDatabaseService db, String query, Map<String, Object> params) {
        ExtendedTestUtil.assertFails(db, query, params, ERROR_NULL_INPUT);
    }
}
