package apoc.script;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestGraphDatabaseFactory;

import javax.script.ScriptEngineManager;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


/**
 * @author Victor Borja vborja@apache.org
 * @since 12.06.16
 */
public class ScriptTest {

    private static GraphDatabaseService db;

    // Test with Oracle nashorn available on JVM 8
    private static final String NASHORN = "nashorn";

    @Rule
    public ExpectedException thrown= ExpectedException.none();


    @BeforeClass
    public static void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure( db, Script.class );
    }

    @AfterClass
    public static void tearDown()
    {
        db.shutdown();
    }


    @Test
    public void testOracleVMHasNashornAvailable() throws Exception {
        ScriptEngineManager sem = new ScriptEngineManager();
        if (null == sem.getEngineByName(NASHORN)) {
            return; /* Skip test, not on Oracle JVM */
        }
        String fullName = sem.getEngineByName(NASHORN).getFactory().getEngineName();
        testCall(db, "CALL apoc.script.engines()", map(),
                (Map<String, Object> map) -> assertEquals(fullName, map.get("name")));
    }

    @Test
    public void testDefaultEngines() throws Exception {
        testResult(db,
                "CALL apoc.script.engines()",
                map(),
                this::assertDefaultEngines);
    }

    private void assertDefaultEngines(Result enginesResult) {
        assertArrayEquals(new String[]{"name", "version", "languageName", "languageVersion", "names"}, enginesResult.columns().toArray());
        ScriptEngineManager sem = new ScriptEngineManager();
        Stream<EnginesResult> expected = sem.getEngineFactories().stream().map(EnginesResult::new);
        Stream<EnginesResult> actual = enginesResult.stream().map(EnginesResult::new);
        assertArrayEquals("should list current vm loaded scripting engines", expected.toArray(), actual.toArray());
    }

    @Test
    public void testEvalJsWithParamsReturningFloat() {
        ScriptEngineManager sem = new ScriptEngineManager();
        if (null == sem.getEngineByName(NASHORN)) {
            return; /* Skip test, not on Oracle JVM */
        }

        testCall(db,
                "RETURN apoc.script.eval({engine}, {code}, {params}) AS answer",
                map("engine", NASHORN, "code", "21 * two", "params", map("two", 2)),
                row -> assertEquals(42.0, row.get("answer")));
    }

    @Test
    public void testEvalJsToBindings() {
        ScriptEngineManager sem = new ScriptEngineManager();
        if (null == sem.getEngineByName(NASHORN)) {
            return; /* Skip test, not on Oracle JVM */
        }

        testCall(db,
                "RETURN apoc.script.evalToBindings({engine}, {code}, {params}, ['x']) AS bindings",
                map("engine", NASHORN, "code", "var x = 10 * two", "params", map("two", 2)),
                row -> {
                    Map<String, Object> bindings = (Map<String, Object>) row.get("bindings");
                    assertEquals(20.0, bindings.get("x"));
                });
    }


}
