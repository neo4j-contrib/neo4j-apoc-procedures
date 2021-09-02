package apoc.util;

import com.google.common.io.Files;
import org.hamcrest.Matcher;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

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
            try {
                assertTrue("Should have an element",res.hasNext());
                Map<String, Object> row = res.next();
                consumer.accept(row);
                assertFalse("Should not have a second element",res.hasNext());
            } catch(Throwable t) {
                printFullStackTrace(t);
                throw t;
            }
        });
    }

    public static void printFullStackTrace(Throwable e) {
        String padding = "";
        while (e != null) {
            if (e.getCause() == null) {
                System.err.println(padding + e.getMessage());
                for (StackTraceElement element : e.getStackTrace()) {
                    if (element.getClassName().matches("^(org.junit|org.apache.maven|sun.reflect|apoc.util.TestUtil|scala.collection|java.lang.reflect|org.neo4j.cypher.internal|org.neo4j.kernel.impl.proc|sun.net|java.net).*"))
                        continue;
                    System.err.println(padding + element.toString());
                }
            }
            e=e.getCause();
            padding += "    ";
        }
    }

    public static void testCallEmpty(GraphDatabaseService db, String call, Map<String,Object> params) {
        testResult(db, call, params, (res) -> assertFalse("Expected no results", res.hasNext()) );
    }

    public static long count(GraphDatabaseService db, String cypher, Map<String, Object> params) {
        return db.executeTransactionally(cypher, params, result -> Iterators.count(result));
    }

    public static long count(GraphDatabaseService db, String cypher) {
        return count(db, cypher, Collections.emptyMap());
    }

    public static void testCallCount( GraphDatabaseService db, String call, final int expected ) {
        testCallCount(db, call, Collections.emptyMap(), expected);
    }

    public static void testCallCount( GraphDatabaseService db, String call, Map<String,Object> params, final int expected ) {
        long count = count(db, call, params);
        assertEquals("expected " + expected + " results, got " + count, (long)expected, count);
    }

    public static void testFail(GraphDatabaseService db, String call, Class<? extends Exception> t) {
        try {
            testResult(db, call, null, (r) -> { while (r.hasNext()) {r.next();} r.close();});
            fail("Didn't fail with "+t.getSimpleName());
        } catch (Exception e) {
            Throwable inner = e;
            boolean found = false;
            do {
                found |= t.isInstance(inner);
                inner = inner.getCause();
            } while (inner!=null && inner.getCause() != inner);
            assertTrue("Didn't fail with "+t.getSimpleName()+" but "+e.getClass().getSimpleName()+" "+e.getMessage(),found);
        }
    }
    public static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
        testResult(db,call,null,resultConsumer);
    }
    public static void testResult(GraphDatabaseService db, String call, Map<String,Object> params, Consumer<Result> resultConsumer) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? Collections.emptyMap() : params;
            Result result = tx.execute(call, p);
            resultConsumer.accept(result);
            tx.commit();
        } catch (RuntimeException e) {
            throw e;
        }
    }

    public static void registerProcedure(GraphDatabaseService db, Class<?>...procedures) {
        GlobalProcedures globalProcedures = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(GlobalProcedures.class);
        for (Class<?> procedure : procedures) {
            try {
                globalProcedures.registerProcedure(procedure, true);
                globalProcedures.registerFunction(procedure, true);
                globalProcedures.registerAggregationFunction(procedure, true);
            } catch (KernelException e) {
                throw new RuntimeException("while registering " + procedure, e);
            }
        }
    }

    public static boolean hasCauses(Throwable t, Class<? extends Throwable>...types) {
        if (anyInstance(t, types)) return true;
        while (t != null && t.getCause() != t) {
            if (anyInstance(t,types)) return true;
            t = t.getCause();
        }
        return false;
    }

    private static boolean anyInstance(Throwable t, Class<? extends Throwable>[] types) {
        for (Class<? extends Throwable> type : types) {
            if (type.isInstance(t)) return true;
        }
        return false;
    }


    public static void ignoreException(Runnable runnable, Class<? extends Throwable>...causes) {
        try {
            runnable.run();
        } catch(Throwable x) {
            if (TestUtil.hasCauses(x,causes)) {
                System.err.println("Ignoring Exception "+x+": "+x.getMessage()+" due to causes "+ Arrays.toString(causes));
            } else {
                throw x;
            }
        }
    }

    public static <T> T assertDuration(Matcher<? super Long> matcher, Supplier<T> function) {
        long start = System.currentTimeMillis();
        T result = null;
        try {
            result = function.get();
        } finally {
            assertThat("duration " + matcher, System.currentTimeMillis()-start, matcher);
            return result;
        }
    }

    public static void assumeRunningInCI() {
        assumeFalse("we're running in CI, so skipping", isRunningInCI());
    }

    public static boolean isRunningInCI() {
        return "true".equals(System.getenv("CI"));
    }

    public static URL getUrlFileName(String filename) {
        return Thread.currentThread().getContextClassLoader().getResource(filename);
    }

    public static String readFileToString(File file) {
        return readFileToString(file, Charset.forName("UTF-8"));
    }

    public static String readFileToString(File file, Charset charset) {
        try {
            return Files.toString(file, charset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> ResourceIterator<T> iteratorSingleColumn(Result result) {
        return result.columnAs(Iterables.single(result.columns()));
    }

    public static <T> T singleResultFirstColumn(GraphDatabaseService db, String cypher) {
        return singleResultFirstColumn(db, cypher, Collections.emptyMap());
    }

    public static <T> T singleResultFirstColumn(GraphDatabaseService db, String cypher, Map<String,Object> params) {
        return db.executeTransactionally(cypher, params, result -> Iterators.singleOrNull(iteratorSingleColumn(result)));
    }

    public static <T> List<T> firstColumn(GraphDatabaseService db, String cypher, Map<String,Object> params) {
        return db.executeTransactionally(cypher , params, result -> Iterators.asList(iteratorSingleColumn(result)));
    }

    public static <T> List<T> firstColumn(GraphDatabaseService db, String cypher) {
        return db.executeTransactionally(cypher , Collections.emptyMap(), result -> Iterators.asList(iteratorSingleColumn(result)));
    }
}
