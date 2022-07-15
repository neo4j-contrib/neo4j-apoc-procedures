package apoc.util;

import com.google.common.io.Files;
import org.hamcrest.Matcher;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
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
            Map<String, Object> p = (params == null) ? Collections.<String, Object>emptyMap() : params;
            resultConsumer.accept(db.execute(call, p));
            tx.success();
        }
    }

    public static void registerProcedure(GraphDatabaseService db, Class<?>...procedures) throws KernelException {
        Procedures proceduresService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);
        for (Class<?> procedure : procedures) {
            proceduresService.registerProcedure(procedure,true);
            proceduresService.registerFunction(procedure, true);
            proceduresService.registerAggregationFunction(procedure, true);
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

    public static void assumeIsCI() {
        assumeFalse( "we're running on CI, so skipping", isCI());
    }

    public static boolean isCI() {
        return "true".equals(System.getenv("CI")) || System.getenv("TEAMCITY_VERSION") != null;
    }

    public static GraphDatabaseBuilder apocGraphDatabaseBuilder() {
        return new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("dbms.backup.enabled","false")
                .setConfig(GraphDatabaseSettings.procedure_unrestricted,"apoc.*");
    }

    public static boolean serverListening(String host, int port)
    {
        try (Socket s = new Socket(host, port)){
            return true;
        } catch (Exception e) {
            return false;
        }
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
}
