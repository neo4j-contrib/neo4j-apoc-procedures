package apoc.custom;

import apoc.RegisterComponentFactory;
import apoc.path.PathExplorer;
import apoc.util.ExtendedTestUtil;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Mode;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static apoc.custom.CypherProcedureTestUtil.startDbWithCustomApocConfigs;
import static apoc.util.ExtendedTestUtil.testRetryCallEventually;
import static apoc.util.MapUtil.map;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Test cases taken and adapted from CypherProceduresStorageTest, with non-deprecated procedures
public class CustomNewProcedureStorageTest {
    private final static String QUERY_CREATE = "RETURN $input1 + $input2 as answer";
    private final static String QUERY_OVERWRITE = "RETURN $input1 + $input2 + 123 as answer";

    @Rule
    public TemporaryFolder STORE_DIR = new TemporaryFolder();

    private DatabaseManagementService dbms;
    private GraphDatabaseService db;
    private GraphDatabaseService sysDb;

    @Before
    public void setUp() throws Exception {
        dbms = startDbWithCustomApocConfigs(STORE_DIR);
        getDbServices();
    }

    private void restartDb() {
        dbms.shutdown();

        dbms = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot().toPath()).build();
        getDbServices();
    }

    private void getDbServices() {
        db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = dbms.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        TestUtil.registerProcedure(db, CypherNewProcedures.class, PathExplorer.class);
    }

    @Test
    public void registerSimpleStatement() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer() :: (answer::LONG)','RETURN 42 as answer')");
        restartDb();
        testCallEventually(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
        testCallEventually(sysDb, "call apoc.custom.show()", row -> {
            assertEquals("answer", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
    }

    @Test
    public void registerSimpleFunctionWithDotInName() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('foo.bar.baz() :: LONG','RETURN 42 as answer')");
        testCallEventually(db, "return custom.foo.bar.baz() as answer", (row) -> assertEquals(42L, row.get("answer")));
        testCallEventually(sysDb, "call apoc.custom.show()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
        restartDb();
        testCallEventually(db, "return custom.foo.bar.baz() as answer", (row) -> assertEquals(42L, row.get("answer")));
        testCallEventually(sysDb, "call apoc.custom.show()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
    }

    @Test
    public void registerSimpleProcedureWithDotInName() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('foo.bar.baz() :: (answer::LONG)','RETURN 42 as answer')");
        testCallEventually(db, "call custom.foo.bar.baz()", (row) -> assertEquals(42L, row.get("answer")));
        testCallEventually(sysDb, "call apoc.custom.show()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
        restartDb();
        testCallEventually(db, "call custom.foo.bar.baz()", (row) -> assertEquals(42L, row.get("answer")));
        testCallEventually(sysDb, "call apoc.custom.show()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
    }

    @Test
    public void registerSimpleStatementConcreteResults() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer() :: (answer::LONG)','RETURN 42 as answer')");
        restartDb();
        testCallEventually(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatement() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer(answer::LONG) :: (answer::LONG)','RETURN $answer as answer')");
        restartDb();
        testCallEventually(db, "call custom.answer(42)", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterStatement() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer(answer::LONG) :: (answer::LONG)','RETURN $answer as answer')");
        restartDb();
        testCallEventually(db, "call custom.answer(42)", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatement() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer(input = 42 ::LONG) :: (answer::LONG)','RETURN $input as answer')");
        restartDb();
        testCallEventually(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypes() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer(int::INTEGER, float::FLOAT,string::STRING,map::MAP,listInt::LIST OF INTEGER,bool::BOOLEAN,date::DATE,datetime::DATETIME,point::POINT) :: (data::LIST OF ANY)','RETURN  [$int,$float,$string,$map,$listInt,$bool,$date,$datetime,$point] as data')");

        restartDb();
        testCallEventually(db, "call custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2}))",
                (row) -> assertEquals(9, ((List) row.get("data")).size()));
    }

    @Test
    public void registerSimpleStatementFunction() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('answer() :: LONG','RETURN 42 as answer')");
        testCallEventually(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
        testCallEventually(sysDb, "call apoc.custom.show()", row -> {
            assertEquals("answer", row.get("name"));
        });

        restartDb();
        testCallEventually(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));

        testCallEventually(sysDb, "call apoc.custom.show()", row -> {
            System.out.println("row = " + row);
            assertEquals("answer", row.get("name"));
        });
    }

    @Test
    public void registerSimpleStatementFunctionWithDotInName() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('foo.bar.baz() :: LONG','RETURN 42 as answer')");
        testCallEventually(db, "return custom.foo.bar.baz() as answer", (row) -> assertEquals(42L, row.get("answer")));
        testCallEventually(sysDb, "call apoc.custom.show()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
        restartDb();
        testCallEventually(db, "return custom.foo.bar.baz() as answer", (row) -> assertEquals(42L, row.get("answer")));
        testCallEventually(sysDb, "call apoc.custom.show()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunction() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('answer() :: LONG','RETURN 42 as answer')");
        restartDb();
        testCallEventually(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunctionUnnamedResultColumn() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('answer() :: LONG','RETURN 42 as answer')");
        restartDb();
        testCallEventually(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatementFunction() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('answer(answer::LONG) :: LONG','RETURN $answer as answer')");
        restartDb();
        testCallEventually(db, "return custom.answer(42) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatementFunction() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('answer(input::LONG) :: LONG','RETURN $input as answer')");
        testCallEventually(db, "return custom.answer(42) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypesFunction() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('answer(int::INTEGER, float::FLOAT,string::STRING,map::MAP,listInt::LIST OF INTEGER,bool::BOOLEAN,date::DATE,datetime::DATETIME,point::POINT) :: LIST OF ANY','RETURN  [$int,$float,$string,$map,$listInt,$bool,$date,$datetime,$point] as data')");
        restartDb();
        testCallEventually(db, "return custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2})) as data",
                (row) -> {
                    assertEquals(9, ((List<List>) row.get("data")).get(0).size());
                });
    }

    @Test
    public void testIssue1744() throws Exception {
        db.executeTransactionally("CREATE (:Area {name: 'foo'})-[:CURRENT]->(:VantagePoint {alpha: 'beta'})");
        sysDb.executeTransactionally("""
                CALL apoc.custom.installProcedure('vantagepoint_within_area(areaName::STRING) :: (resource::NODE)',
                  "MATCH (start:Area {name: $areaName} )
                    CALL apoc.path.expand(start,'CONTAINS>|<SEES|CURRENT','',0,100) YIELD path
                    UNWIND nodes(path) as node
                    WITH node
                    WHERE node:VantagePoint
                    RETURN DISTINCT node as resource",
                    "neo4j",
                  'READ');""");

        // function analogous to procedure
        sysDb.executeTransactionally("""
                CALL apoc.custom.installFunction('vantagepoint_within_area(areaName::STRING) ::NODE',
                  "MATCH (start:Area {name: $areaName} )
                    CALL apoc.path.expand(start,'CONTAINS>|<SEES|CURRENT','',0,100) YIELD path
                    UNWIND nodes(path) as node
                    WITH node
                    WHERE node:VantagePoint
                    RETURN DISTINCT node as resource");""");

        testCallIssue1744();
        restartDb();

        final String logFileContent = Files.readString(new File(FileUtils.getLogDirectory(), "debug.log").toPath());
        assertFalse(logFileContent.contains("Could not register function: custom.vantagepoint_within_area"));
        assertFalse(logFileContent.contains("Could not register procedure: custom.vantagepoint_within_area"));
        testCallIssue1744();
    }

    private void testCallIssue1744() {
        testCallEventually(db, "CALL custom.vantagepoint_within_area('foo')", this::assertCallIssue1744);
        testCallEventually(db, "RETURN custom.vantagepoint_within_area('foo') as resource", this::assertCallIssue1744);
    }

    private void assertCallIssue1744(Map<String, Object> row) {
        final Node resource = (Node) row.get("resource");
        assertEquals("VantagePoint", resource.getLabels().iterator().next().name());
        assertEquals("beta", resource.getProperty("alpha"));
    }

    @Test
    @Ignore("Ignored because of https://trello.com/c/XWc7tBAb/74-custom-procedures-with-overload-fail-after-refresh")
    public void testMultipleOverrideWithFunctionAndProcedures() throws Exception {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('override() :: (result::LONG)','RETURN 42 as result')");

        // function homonym to procedure
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('override() :: LONG','RETURN 10 as answer')");

        // get fun/proc created
        testCallEventually(db, "RETURN custom.override() as result", r -> {
            assertEquals(10L, r.get("result"));
        });
        testCallEventually(db, "CALL custom.override()", r -> {
            assertEquals(42L, r.get("result"));
        });

        // overrides functions and procedures homonym to the previous ones
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('override(input::INT) :: INT', 'RETURN $input + 2 AS result')");
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('override(input::INT) :: INT', 'RETURN $input AS result')");

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('override(input::INT) :: (result::INT)', 'RETURN $input AS result')");
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('override(input::INT) :: (result::INT)', 'RETURN $input + 2 AS result')");

        // get fun/proc updated
        testCallEventually(db, "RETURN custom.override(3) as result", r -> {
            assertEquals(3L, r.get("result"));
        });
        testCallEventually(db, "CALL custom.override(2)", r -> {
            assertEquals(4L, r.get("result"));
        });
        restartDb();

        final String logFileContent = Files.readString(new File(FileUtils.getLogDirectory(), "debug.log").toPath());
        assertFalse(logFileContent.contains("Could not register function: custom.override"));
        assertFalse(logFileContent.contains("Could not register procedure: custom.override"));

        // override after restart
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('override(input::INT) :: INT', 'RETURN $input + 1 AS result')");
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('override(input::INT) :: (result::INT)', 'RETURN $input + 3 AS result')");

        // get fun/proc updated
        testCallEventually(db, "RETURN custom.override(3) as result", r -> {
            assertEquals(4L, r.get("result"));
        });
        testCallEventually(db, "CALL custom.override(2)", r -> {
            assertEquals(5L, r.get("result"));
        });
    }

    @Test
    public void functionSignatureShouldNotChangeBeforeAndAfterRestart() {
        functionsCreation();
        functionAssertions();
        restartDb();
        functionAssertions();
    }

    @Test
    public void procedureSignatureShouldNotChangeBeforeAndAfterRestart() {
        proceduresCreation();
        procedureAssertions();
        restartDb();
        procedureAssertions();
    }

    @Test
    public void functionSignatureShouldNotChangeBeforeAndAfterRestartAndOverwrite() {
        functionsCreation();
        functionAssertions();
        restartDb();

        // overwrite function
        sysDb.executeTransactionally("call apoc.custom.installFunction('sumFun2(input1::INT, input2::INT) :: INT',$query)",
                map("query", QUERY_OVERWRITE));
        sysDb.executeTransactionally("call apoc.custom.installFunction('sumFun1(input1 = null::INT, input2 = null::INT) :: INT',$query)",
                map("query", QUERY_OVERWRITE));
        functionAssertions();
    }

    @Test
    public void procedureSignatureShouldNotChangeBeforeAndAfterRestartAndOverwrite() {
        proceduresCreation();
        procedureAssertions();
        restartDb();

        // overwrite function
        sysDb.executeTransactionally("call apoc.custom.installProcedure('sum1(input1 = null::INT, input2 = null::INT) :: (answer::INT)',$query)",
                map("query", QUERY_OVERWRITE));
        sysDb.executeTransactionally("call apoc.custom.installProcedure('sum2(input1::INT, input2::INT) :: (answer::INT)',$query)",
                map("query", QUERY_OVERWRITE));
        procedureAssertions();
    }

    @Test
    public void testIssue1714WithRestartDb() throws Exception {
        db.executeTransactionally("CREATE (i:Target {value: 2});");
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('nn(val::INTEGER) :: NODE', 'MATCH (t:Target {value : $val}) RETURN t')");
        restartDb();
        TestUtil.testCall(db, "RETURN custom.nn(2) as row", (row) -> assertEquals(2L, ((Node) row.get("row")).getProperty("value")));
    }

    // regression tests for https://github.com/neo4j-contrib/neo4j-apoc-procedures/issues/4570 :
    // installing the same name into a different db must not remove/overwrite another db's entry.
    // CypherHandlerNewProcedure is exercised directly (bypassing the `apoc.custom.install*` procedures'
    // target-database validation) so the storage-layer coexistence/signature-compatibility logic can be
    // tested against arbitrary db-name strings without needing real additional databases.

    @Test
    public void installingSameNamedProcedureInTwoDbsCoexists() {
        ProcedureSignature signature = new Signatures(CypherProceduresHandler.PREFIX)
                .asProcedureSignature("multiDbProc() :: (answer::INT)", "", Mode.READ);

        CypherHandlerNewProcedure.installProcedure("dbone", signature, "RETURN 1 as answer");
        CypherHandlerNewProcedure.installProcedure("dbtwo", signature, "RETURN 2 as answer");

        assertEquals("RETURN 1 as answer", statementOf("dbone", "multiDbProc"));
        assertEquals("RETURN 2 as answer", statementOf("dbtwo", "multiDbProc"));

        CypherHandlerNewProcedure.dropProcedure("dbone", "multiDbProc");
        CypherHandlerNewProcedure.dropProcedure("dbtwo", "multiDbProc");
    }

    @Test
    public void installingSameNamedFunctionInTwoDbsCoexists() {
        UserFunctionSignature signature = new Signatures(CypherProceduresHandler.PREFIX)
                .asFunctionSignature("multiDbFun() :: INT", "");

        CypherHandlerNewProcedure.installFunction("dbone", signature, "RETURN 1 as answer", false);
        CypherHandlerNewProcedure.installFunction("dbtwo", signature, "RETURN 2 as answer", false);

        assertEquals("RETURN 1 as answer", statementOf("dbone", "multiDbFun"));
        assertEquals("RETURN 2 as answer", statementOf("dbtwo", "multiDbFun"));

        CypherHandlerNewProcedure.dropFunction("dbone", "multiDbFun");
        CypherHandlerNewProcedure.dropFunction("dbtwo", "multiDbFun");
    }

    @Test
    public void installingIncompatibleSignatureProcedureInAnotherDbFails() {
        ProcedureSignature signature = new Signatures(CypherProceduresHandler.PREFIX)
                .asProcedureSignature("mismatchProc() :: (answer::INT)", "", Mode.READ);
        CypherHandlerNewProcedure.installProcedure("dbone", signature, "RETURN 1 as answer");

        ProcedureSignature incompatible = new Signatures(CypherProceduresHandler.PREFIX)
                .asProcedureSignature("mismatchProc(xx :: STRING) :: (answer::INT)", "", Mode.READ);
        try {
            CypherHandlerNewProcedure.installProcedure("dbtwo", incompatible, "RETURN 1 as answer");
            fail("Should fail because of incompatible signature already registered in another db");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("incompatible signature"));
        }

        CypherHandlerNewProcedure.dropProcedure("dbone", "mismatchProc");
    }

    @Test
    public void installingIncompatibleSignatureFunctionInAnotherDbFails() {
        UserFunctionSignature signature = new Signatures(CypherProceduresHandler.PREFIX)
                .asFunctionSignature("mismatchFun() :: INT", "");
        CypherHandlerNewProcedure.installFunction("dbone", signature, "RETURN 1 as answer", false);

        UserFunctionSignature incompatible = new Signatures(CypherProceduresHandler.PREFIX)
                .asFunctionSignature("mismatchFun() :: STRING", "");
        try {
            CypherHandlerNewProcedure.installFunction("dbtwo", incompatible, "RETURN '1' as answer", false);
            fail("Should fail because of incompatible signature already registered in another db");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("incompatible signature"));
        }

        CypherHandlerNewProcedure.dropFunction("dbone", "mismatchFun");
    }

    // Migration scenario: a database upgraded from a version where cross-db signature compatibility was
    // never enforced (e.g. data written via the deprecated apoc.custom.declareProcedure/declareFunction,
    // which has no such check even today) may already contain two databases' worth of registrations for the
    // same name with genuinely incompatible signatures. CypherProceduresHandler.registerProcedure/registerFunction
    // is exercised directly (as restoreProceduresAndFunctions() would call it for each database's own node)
    // to simulate that pre-existing, inconsistent data without needing real additional databases.
    @Test
    public void conflictingLegacyProcedureSignaturesDoNotFlapBetweenDbs() {
        CypherProceduresHandler handler = cypherProceduresHandler();

        ProcedureSignature ownSignature = new Signatures(CypherProceduresHandler.PREFIX)
                .asProcedureSignature("legacyConflictProc() :: (answer::INT)", "", Mode.READ);
        ProcedureSignature otherSignature = new Signatures(CypherProceduresHandler.PREFIX)
                .asProcedureSignature("legacyConflictProc(xx :: STRING) :: (answer::INT)", "", Mode.READ);

        // this db's own (legitimate) registration
        assertTrue(handler.registerProcedure(ownSignature, "RETURN 1 as answer", null));
        testCallEventually(db, "CALL custom.legacyConflictProc()", row -> assertEquals(1L, row.get("answer")));

        // a pre-existing, incompatible registration for a different (simulated) db must not overwrite it
        assertTrue(handler.registerProcedure(otherSignature, "RETURN 2 as answer", "otherDb"));
        testCallEventually(db, "CALL custom.legacyConflictProc()", row -> assertEquals(1L, row.get("answer")));

        // simulating a periodic refresh re-processing both (unchanged) entries must not flap either
        assertTrue(handler.registerProcedure(otherSignature, "RETURN 2 as answer", "otherDb"));
        assertTrue(handler.registerProcedure(ownSignature, "RETURN 1 as answer", null));
        testCallEventually(db, "CALL custom.legacyConflictProc()", row -> assertEquals(1L, row.get("answer")));

        handler.registerProcedure(ownSignature, null, null);
        handler.registerProcedure(otherSignature, null, "otherDb");
    }

    @Test
    public void conflictingLegacyFunctionSignaturesDoNotFlapBetweenDbs() {
        CypherProceduresHandler handler = cypherProceduresHandler();

        UserFunctionSignature ownSignature = new Signatures(CypherProceduresHandler.PREFIX)
                .asFunctionSignature("legacyConflictFun() :: INT", "");
        UserFunctionSignature otherSignature = new Signatures(CypherProceduresHandler.PREFIX)
                .asFunctionSignature("legacyConflictFun() :: STRING", "");

        assertTrue(handler.registerFunction(ownSignature, "RETURN 1 as answer", false, false, null));
        testCallEventually(db, "RETURN custom.legacyConflictFun() as answer", row -> assertEquals(1L, row.get("answer")));

        assertTrue(handler.registerFunction(otherSignature, "RETURN '2' as answer", false, false, "otherDb"));
        testCallEventually(db, "RETURN custom.legacyConflictFun() as answer", row -> assertEquals(1L, row.get("answer")));

        assertTrue(handler.registerFunction(otherSignature, "RETURN '2' as answer", false, false, "otherDb"));
        assertTrue(handler.registerFunction(ownSignature, "RETURN 1 as answer", false, false, null));
        testCallEventually(db, "RETURN custom.legacyConflictFun() as answer", row -> assertEquals(1L, row.get("answer")));

        handler.registerFunction(ownSignature, null, false, false, null);
        handler.registerFunction(otherSignature, null, false, false, "otherDb");
    }

    private CypherProceduresHandler cypherProceduresHandler() {
        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle =
                ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(RegisterComponentFactory.RegisterComponentLifecycle.class);
        return (CypherProceduresHandler) registerComponentLifecycle.getResolvers().get(CypherProceduresHandler.class).get(db.databaseName());
    }

    // reads directly from the system db (bypassing `apoc.custom.show`, which validates that
    // `databaseName` is a real, existing database via `SHOW DATABASES`)
    private String statementOf(String databaseName, String name) {
        try (Transaction tx = sysDb.beginTx()) {
            return CypherHandlerNewProcedure.show(databaseName, tx)
                    .filter(info -> info.name.equals(name))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No custom procedure/function named " + name + " found for db " + databaseName))
                    .statement;
        }
    }

    private void functionsCreation() {
        // 1 declareFunction with default null, 1 declareFunction without default
        sysDb.executeTransactionally("call apoc.custom.installFunction('sumFun1(input1 = null::INT, input2 = null::INT) :: INT',$query)",
                map("query", QUERY_CREATE));
        sysDb.executeTransactionally("call apoc.custom.installFunction('sumFun2(input1::INT, input2::INT) :: INT',$query)",
                map("query", QUERY_CREATE));
    }
    
    private void functionAssertions() {
        testResultEventually(db, "SHOW FUNCTIONS YIELD signature, name WHERE name STARTS WITH 'custom.sumFun' RETURN DISTINCT name, signature ORDER BY name",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals("custom.sumFun1(input1 = null :: INTEGER, input2 = null :: INTEGER) :: INTEGER", row.get("signature"));
                    row = r.next();
                    assertEquals("custom.sumFun2(input1 :: INTEGER, input2 :: INTEGER) :: INTEGER", row.get("signature"));
                    assertFalse(r.hasNext());
                });
        testResultEventually(sysDb, "call apoc.custom.show() YIELD name RETURN name ORDER BY name",
                row -> {
                    final List<String> sumFun1 = List.of("sumFun1", "sumFun2");
                    assertEquals(sumFun1, Iterators.asList(row.columnAs("name")));
                });

        TestUtil.testCall(db, String.format("RETURN %s()", "custom.sumFun1"), (row) -> assertNull(row.get("answer")));
        try {
            TestUtil.testCall(db, String.format("RETURN %s()", "custom.sumFun2"), (row) -> fail("Should fail because of missing params"));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Function call does not provide the required number of arguments: expected 2 got 0"));
        }
    }

    private void proceduresCreation() {
        // 1 declareProcedure with default null, 1 declareProcedure without default
        sysDb.executeTransactionally("call apoc.custom.installProcedure('sum1(input1 = null::INT, input2 = null::INT) :: (answer::INT)',$query)",
                map("query", QUERY_CREATE));
        sysDb.executeTransactionally("call apoc.custom.installProcedure('sum2(input1::INT, input2::INT) :: (answer::INT)',$query)",
                map("query", QUERY_CREATE));
    }

    private void procedureAssertions() {
        testResultEventually(db, "SHOW PROCEDURES YIELD signature, name WHERE name STARTS WITH 'custom.sum' RETURN DISTINCT name, signature ORDER BY name",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals("custom.sum1(input1 = null :: INTEGER, input2 = null :: INTEGER) :: (answer :: INTEGER)", row.get("signature"));
                    row = r.next();
                    assertEquals("custom.sum2(input1 :: INTEGER, input2 :: INTEGER) :: (answer :: INTEGER)", row.get("signature"));
                    assertFalse(r.hasNext());
                });
        testResultEventually(sysDb, "call apoc.custom.show() YIELD name RETURN name ORDER BY name",
                row -> {
                    final List<String> sumFun1 = List.of("sum1", "sum2");
                    assertEquals(sumFun1, Iterators.asList(row.columnAs("name")));
                });

        testCallEventually(db, String.format("CALL %s", "custom.sum1"), (row) -> assertNull(row.get("answer")));

        try {
            TestUtil.testCall(db, String.format("CALL %s()", "custom.sum2"), (row) -> fail("Should fail because of missing params"));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Procedure call does not provide the required number of arguments: got 0 expected at least 2"));
        }
    }

    private void testResultEventually(GraphDatabaseService db, String call, Consumer<Result> consumer) {
        ExtendedTestUtil.testResultEventually(db, call, consumer, TIMEOUT);
    }

    private void testCallEventually(GraphDatabaseService db, String call, Consumer<Map<String, Object>> consumer) {
        testRetryCallEventually(db, call, consumer, TIMEOUT);
    }
}
