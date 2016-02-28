package apoc.util;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
//import org.neo4j.kernel.internal.GraphDatabaseAPI;

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
            tx.success();
//            res.close();
        }
    }

    static void registerProcedure(GraphDatabaseService db, Class<?> procedure) throws KernelException {
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(procedure);
    }
}
