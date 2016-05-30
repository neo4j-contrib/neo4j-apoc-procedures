package apoc.util;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

//import org.neo4j.kernel.GraphDatabaseAPI;

/**
 * @author mh
 * @since 26.02.16
 */
public class TestUtil {
    public static void testCall(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer) {
        testCall(db,call,null,consumer);
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

    public static void testCallEmpty(GraphDatabaseService db, String call, Map<String,Object> params) {
        testResult(db, call, params, (res) -> assertFalse("Expected no results", res.hasNext()) );
    }

    public static void testCallCount( GraphDatabaseService db, String call, Map<String,Object> params, final int count ) {
        testResult( db, call, params, ( res ) -> {
            int left = count;
            while ( left > 0 ) {
                assertTrue( "Expected " + count + " results, but got only " + (count - left), res.hasNext() );
                res.next();
                left--;
            }
            assertFalse( "Expected " + count + " results, but there are more ", res.hasNext() );
        } );
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

    public static boolean hasCause(Throwable t, Class<? extends Throwable>type) {
        if (type.isInstance(t)) return true;
        while (t != null && t.getCause() != t) {
            if (type.isInstance(t)) return true;
            t = t.getCause();
        }
        return false;
    }

    public static void ignoreException(Runnable runnable, Class<? extends Throwable> cause) {
        try {
            runnable.run();
        } catch(Throwable x) {
            if (TestUtil.hasCause(x,cause)) {
                System.err.println("Ignoring Exception "+x+": "+x.getMessage()+" due to cause "+cause);
            } else {
                throw x;
            }
        }
    }
}
