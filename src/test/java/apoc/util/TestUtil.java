package apoc.util;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;

/**
 * @author mh
 * @since 26.02.16
 */
public class TestUtil {
    static void testCall(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer) {
        try (Transaction tx = db.beginTx()) {
            Result res = db.execute(call);

            if (res.hasNext()) {
                Map<String, Object> row = res.next();
                consumer.accept(row);
            }
            assertFalse(res.hasNext());
            res.close();
            tx.success();
        }
    }
}
