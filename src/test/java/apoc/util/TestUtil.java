package apoc.util;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
//import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;

/**
 * @author mh
 * @since 26.02.16
 */
public class TestUtil {
    public static void testCall(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer) {
        testCall(db,call,null,consumer);
    }

    public static Map<String,Object> map(Object ... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i+=2) {
            map.put(values[i].toString(),values[i+1]);
        }
        return map;
    }

    public static void testCall(GraphDatabaseService db, String call,Map<String,Object> params, Consumer<Map<String, Object>> consumer) {
        testResult(db, call, params, (res) -> {
            if (res.hasNext()) {
                Map<String, Object> row = res.next();
                consumer.accept(row);
            }
            assertFalse(res.hasNext());
        });
    }

    public static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
        testResult(db,call,null,resultConsumer);
    }
    public static void testResult(GraphDatabaseService db, String call, Map<String,Object> params, Consumer<Result> resultConsumer) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? Collections.<String, Object>emptyMap() : params;
            resultConsumer.accept(db.execute(call, p));
            tx.success();
        }
    }

    public static void registerProcedure(GraphDatabaseService db, Class<?> procedure) throws KernelException {
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(procedure);
    }
}
