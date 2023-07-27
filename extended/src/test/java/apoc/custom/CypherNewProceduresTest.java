package apoc.custom;

import apoc.RegisterComponentFactory;
import apoc.SystemPropertyKeys;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.ExtendedSystemLabels.ApocCypherProcedures;
import static apoc.custom.CypherProcedureTestUtil.assertProcedureFails;
import static apoc.custom.CypherProcedureTestUtil.startDbWithCustomApocConfigs;
import static apoc.custom.CypherProceduresHandler.FUNCTION;
import static apoc.custom.CypherProceduresHandler.PROCEDURE;
import static apoc.custom.Signatures.SIGNATURE_SYNTAX_ERROR;
import static apoc.util.ExtendedTestUtil.testRetryCallEventually;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.SystemDbUtil.BAD_TARGET_ERROR;
import static apoc.util.SystemDbUtil.NON_SYS_DB_ERROR;
import static apoc.util.SystemDbUtil.PROCEDURE_NOT_ROUTED_ERROR;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testCallCountEventually;
import static apoc.util.TestUtil.testResult;
import static apoc.util.TestUtil.waitDbsAvailable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CypherNewProceduresTest {

    private static final File directory = new File("target/conf");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void beforeClass() throws Exception {
        databaseManagementService = startDbWithCustomApocConfigs(storeDir);

        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        TestUtil.registerProcedure(sysDb, CypherNewProcedures.class);
        TestUtil.registerProcedure(db, CypherProcedures.class);
    }

    @AfterClass
    public static void afterClass() {
        databaseManagementService.shutdown();
    }

    @After
    public void after() throws Exception {
        sysDb.executeTransactionally("CALL apoc.custom.dropAll('neo4j')");
        testCallCountEventually(db, "CALL apoc.custom.list", 0, TIMEOUT);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    //
    // test cases taken and adapted from CypherProceduresTest.java
    //

    @Test
    public void registerSimpleStatement() throws Exception {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer2() :: (answer::INT)','RETURN 42 as answer')");
        testCallEventually("CALL custom.answer2()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerSimpleStatementWithOneChar() {
        try {
            testCall(sysDb, "CALL apoc.custom.installProcedure('b() :: (answer::INT)','RETURN 42 as answer')",
                    r -> fail("Should fail because of one char procedure name"));
        } catch (QueryExecutionException e) {
            String message = e.getMessage();
            assertTrue("Actual err. message is: " + message,
                    message.contains("Note that procedure/function name, possible map keys, input and output names must have at least 2 character"));
        }
    }

    @Test
    public void shouldInstallProcedureWithDefaultListAndMaps() {
        sysDb.executeTransactionally("call apoc.custom.installProcedure('procWithFloatList(minScore = [1.1,2.2,3.3] :: LIST OF FLOAT) :: (res :: BOOLEAN, first :: FLOAT)',\n" +
                                  "    'return size($minScore) < 4 as res, $minScore[0] as first')");
        testCallEventually("call custom.procWithFloatList", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals(1.1D, (double) row.get("first"), 0.1D);
        });
        testCallEventually( "call custom.procWithFloatList([9.1, 2.6, 3.1, 4.3, 5.5])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals(9.1D, (double) row.get("first"), 0.1D);
        });

        sysDb.executeTransactionally("call apoc.custom.installProcedure('procWithIntList(minScore = [1,2,3] :: LIST OF INT) :: (res :: BOOLEAN, first :: FLOAT)',\n" +
                                  "    'return size($minScore) < 4 as res, toInteger($minScore[0]) as first')");
        testCallEventually( "call custom.procWithIntList", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals(1L, row.get("first"));
        });
        testCallEventually("call custom.procWithIntList([9,2,3,4,5])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals(9L, row.get("first"));
        });

        sysDb.executeTransactionally("call apoc.custom.installProcedure('procWithListString(minScore = [\"1\",\"2\",\"3\"] :: LIST OF STRING) :: (res :: BOOLEAN, first :: FLOAT)',\n" +
                                  "    'return size($minScore) < 4 as res, $minScore[0] + \" - suffix\" as first ')");
        testCallEventually("call custom.procWithListString", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals("1 - suffix", row.get("first"));
        });
        testCallEventually("call custom.procWithListString(['aaa','bbb','ccc','ddd','eee'])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals("aaa - suffix", row.get("first"));
        });

        sysDb.executeTransactionally("call apoc.custom.installProcedure('procWithListPlainString(minScore = [1, 2, 3] :: LIST OF STRING) :: (res :: BOOLEAN, first :: FLOAT)',\n" +
                                  "    'return size($minScore) < 4 as res, $minScore[0] + \" - suffix\" as first ')");
        testCallEventually( "call custom.procWithListPlainString", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals("1 - suffix", row.get("first"));
        });
        testCallEventually("call custom.procWithListPlainString(['aaa','bbb','ccc','ddd','eee'])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals("aaa - suffix", row.get("first"));
        });

        sysDb.executeTransactionally("call apoc.custom.installProcedure(\"procWithListStringQuoted(minScore = ['1','2','3'] :: LIST OF STRING) :: (res :: BOOLEAN, first :: FLOAT)\",\n" +
                                  "    'return size($minScore) < 4 as res, $minScore[0] + \" - suffix\" as first ')");
        testCallEventually("call custom.procWithListStringQuoted", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals("1 - suffix", row.get("first"));
        });
        testCallEventually("call custom.procWithListStringQuoted(['aaa','bbb','ccc','ddd','eee'])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals("aaa - suffix", row.get("first"));
        });

        sysDb.executeTransactionally("call apoc.custom.installProcedure('procWithListStringVars(minScore = [true,false,null] :: LIST OF STRING) :: (res :: BOOLEAN, first :: STRING)',\n" +
                                  "    'return size($minScore) < 4 as res, $minScore[0] as first ')");
        testCallEventually("call custom.procWithListStringVars", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals("true", row.get("first"));
        });
        testCallEventually("call custom.procWithListStringVars(['aaa','bbb','ccc','ddd','eee'])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals("aaa", row.get("first"));
        });

        sysDb.executeTransactionally("call apoc.custom.installProcedure('procWithMapList(minScore = {aa: 1, bb: \"2\"} :: MAP) :: (res :: MAP, first :: ANY)',\n" +
                                  "    'return $minScore as res, $minScore[\"a\"] as first ')");
        testCallEventually("call custom.procWithMapList", (row) -> {
            assertEquals(Map.of("aa", 1L, "bb", "2"), row.get("res"));
        });
        testCallEventually("call custom.procWithMapList({c: true})", (row) -> {
            assertEquals(Map.of("c", true), row.get("res"));
        });
    }

    @Test
    public void shouldInstallFunctionWithDefaultListAndMaps() {
        sysDb.executeTransactionally("call apoc.custom.installFunction('funWithFloatList(minScore = [1.1,2.2,3.3] :: LIST OF FLOAT) :: FLOAT',\n" +
                                  "    'return $minScore[0]')");
        testCallEventually( "RETURN custom.funWithFloatList() AS res",
                (row) -> assertEquals(1.1D, (double) row.get("res"), 0.1D));
        testCallEventually( "RETURN custom.funWithFloatList([9.1, 2.6, 3.1, 4.3, 5.5]) AS res",
                (row) -> assertEquals(9.1D, (double) row.get("res"), 0.1D));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('funWithIntList(minScore = [1,2,3] :: LIST OF INT) :: BOOLEAN',\n" +
                                  "    'return size($minScore) < 4')");
        testCallEventually("RETURN custom.funWithIntList() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCallEventually("RETURN custom.funWithIntList([9,2,3,4,5]) AS res",
                (row) -> assertEquals(false, row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('funWithListString(minScore = [\"1\",\"2\",\"3\"] :: LIST OF STRING) :: BOOLEAN',\n" +
                                  "    'return size($minScore) < 4')");
        testCallEventually("RETURN custom.funWithListString() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCallEventually("RETURN custom.funWithListString(['aaa','bbb','ccc','ddd','eee']) AS res",
                (row) -> assertEquals(false, row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('funWithListStringPlain(minScore = [1, 2, 3] :: LIST OF STRING) :: BOOLEAN',\n" +
                                  "    'return size($minScore) < 4')");
        testCallEventually("RETURN custom.funWithListStringPlain() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCallEventually( "RETURN custom.funWithListStringPlain(['aaa','bbb','ccc','ddd','eee']) AS res",
                (row) -> assertEquals(false, row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction(\"funWithListStringQuoted(minScore = ['1','2','3'] :: LIST OF STRING) :: BOOLEAN\",\n" +
                                  "    'return size($minScore) < 4')");
        testCallEventually( "RETURN custom.funWithListStringQuoted() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCallEventually( "RETURN custom.funWithListStringQuoted(['aaa','bbb','ccc','ddd','eee']) AS res",
                (row) -> assertEquals(false, row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('funWithListStringVars(minScore = [true,false,null] :: LIST OF STRING) :: BOOLEAN',\n" +
                                  "    'return size($minScore) < 4')");
        testCallEventually("RETURN custom.funWithListStringVars() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCallEventually("RETURN custom.funWithListStringVars(['aaa','bbb','ccc','ddd','eee']) AS res",
                (row) -> assertEquals(false, row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('funWithMapList(minScore = {aa: 1, bb: \"2\"} :: MAP) :: MAP',\n" +
                                  "    'return $minScore AS mapRes')");
        testCallEventually("RETURN custom.funWithMapList() AS res",
                (row) -> assertEquals(Map.of("mapRes", Map.of("aa", 1L, "bb", "2")), row.get("res")));
        testCallEventually("RETURN custom.funWithMapList({c: true}) AS res",
                (row) -> assertEquals(Map.of("mapRes", Map.of("c", true)), row.get("res")));
    }

    @Test
    public void shouldInstallProcedureWithDefaultString() {
        String query = "RETURN $minScore + ' - suffix' as res";
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure(\"procWithSingleQuotedText(minScore=' foo \\\" bar '::STRING)::(res::STRING)\", $query)",
                Map.of("query", query));
        testCallEventually("CALL custom.procWithSingleQuotedText", (row) -> {
            assertEquals(" foo \" bar  - suffix", row.get("res"));
        });

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('procWithDoubleQuotedText(minScore=\" foo \\' bar \"::STRING) :: (res::STRING)', $query)",
                Map.of("query", query));
        testCallEventually("CALL custom.procWithDoubleQuotedText", (row) -> {
            assertEquals(" foo ' bar  - suffix", row.get("res"));
        });
        testCallEventually("CALL custom.procWithDoubleQuotedText('myText')", (row) -> assertEquals("myText - suffix", row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('procWithPlainText(minScore = plainText :: STRING) :: (res::STRING)', $query)",
                Map.of("query", query));
        testCallEventually("CALL custom.procWithPlainText", (row) -> assertEquals("plainText - suffix", row.get("res")));
        testCallEventually("CALL custom.procWithPlainText('myText')", (row) -> assertEquals("myText - suffix", row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('procWithStringNull(minScore = null :: STRING) :: (res :: STRING)', $query)",
                Map.of("query", query));
        testCallEventually("CALL custom.procWithStringNull", (row) -> assertNull(row.get("res")));
        testCallEventually("CALL custom.procWithStringNull('other')", (row) -> assertEquals("other - suffix", row.get("res")));
    }

    @Test
    public void shouldInstallFunctionWithDefaultString() {
        String query = "RETURN $minScore + ' - suffix' as res";
        sysDb.executeTransactionally("CALL apoc.custom.installFunction(\"funWithSingleQuotedText(minScore=' foo \\\" bar '::STRING):: STRING\", $query)",
                Map.of("query", query));
        testCallEventually("RETURN custom.funWithSingleQuotedText() AS res", (row) -> {
            assertEquals(" foo \" bar  - suffix", row.get("res"));
        });

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('funWithDoubleQuotedText(minScore=\" foo \\' bar \"::STRING) :: STRING', $query)",
                Map.of("query", query));
        testCallEventually("RETURN custom.funWithDoubleQuotedText() AS res", (row) -> {
            assertEquals(" foo ' bar  - suffix", row.get("res"));
        });
        testCallEventually("RETURN custom.funWithDoubleQuotedText('myText') AS res", (row) -> assertEquals("myText - suffix", row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('funWithPlainText(minScore = plainText :: STRING) :: STRING', $query)",
                Map.of("query", query));
        testCallEventually("RETURN custom.funWithPlainText() AS res", (row) -> assertEquals("plainText - suffix", row.get("res")));
        testCallEventually("RETURN custom.funWithPlainText('myText') AS res", (row) -> assertEquals("myText - suffix", row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('funWithStringNull(minScore = null :: STRING) :: STRING', $query)",
                Map.of("query", query));
        testCallEventually("RETURN custom.funWithStringNull() AS res", (row) -> assertNull(row.get("res")));
        testCallEventually("RETURN custom.funWithStringNull('other') AS res", (row) -> assertEquals("other - suffix", row.get("res")));
    }

    @Test
    public void overrideSingleCallStatement() throws Exception {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer() :: (answer::ANY)','RETURN 42 as answer')");
        db.executeTransactionally("call db.clearQueryCaches()");
        testCallEventually("call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer() :: (answer::ANY)','RETURN 43 as answer')");
        db.executeTransactionally("call db.clearQueryCaches()");
        testCallEventually("call custom.answer()", (row) -> assertEquals(43L, row.get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResults() throws Exception {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer() :: (answer::LONG)','RETURN 42 as answer')");
        testCallEventually("call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatement() throws Exception {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answerAny(input::ANY) :: (answer::INT)','RETURN $input as answer')");
        testCallEventually("call custom.answerAny(42)", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterStatement() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer(input::NUMBER) :: (answer::INT)','RETURN $input as answer')");
        testCallEventually("call custom.answer(42)", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatement()  {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer(input = 42 :: INT) :: (answer::INT)','RETURN $input as answer')");
        testCallEventually("call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testValidationProceduresIssue2654() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('doubleProc(input::INT) :: (answer::INT)', 'RETURN $input * 2 AS answer')");
        testCallEventually("CALL custom.doubleProc(4);", (r) -> assertEquals(8L, r.get("answer")));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('testValTwo(input::INT) :: (answer::INT)', 'RETURN $input ^ 2 AS answer')");
        testCallEventually("CALL custom.testValTwo(4);", (r) -> assertEquals(16D, r.get("answer")));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('testValThree(input::MAP, power :: LONG) :: (answer::INT)', 'RETURN $input.a ^ $power AS answer')");
        testCallEventually("CALL custom.testValThree({a: 2}, 3);", (r) -> assertEquals(8D, r.get("answer")));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure($signature, $query)",
                Map.of("signature", "testValFour(input::INT, power::NUMBER) :: (answer::INT)",
                        "query", "UNWIND range(0, $power) AS power RETURN $input ^ power AS answer"));

        testCallEventually("CALL custom.testValFour(2, 3) YIELD answer RETURN collect(answer) AS res",
                (r) -> assertEquals(List.of(1D, 2D, 4D, 8D), r.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure($signature, $query)",
                Map.of("signature", "multiProc(input::LOCALDATETIME, minus::INT) :: (first::INT, second:: STRING, third::DATETIME)",
                        "query", "WITH $input AS input RETURN input.year - $minus AS first, toString(input) as second, input as third"));

        testCallEventually("CALL custom.multiProc(localdatetime('2020'), 3);", (r) -> {
            assertEquals(2017L, r.get("first"));
            assertEquals("2020-01-01T00:00:00", r.get("second"));
            assertEquals(LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0), r.get("third"));
        });
    }

    @Test
    public void testValidationFunctionsIssue2654() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('double(input::INT) :: INT', 'RETURN $input * 2 AS answer')");
        testCallEventually("RETURN custom.double(4) AS answer", (r) -> assertEquals(8L, r.get("answer")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('testValOne(input::INT) :: INT', 'RETURN $input ^ 2 AS answer')");
        testCallEventually("RETURN custom.testValOne(3) as result", (r) -> assertEquals(9D, r.get("result")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction($signature, $query)",
                Map.of("signature", "multiFun(point:: POINT, input ::DATETIME, duration :: DURATION, minus = 1 ::INT) :: STRING",
                        "query", "RETURN toString($duration) + ', ' + toString($input.epochMillis - $minus) + ', ' + toString($point) as result"));
        testCallEventually("RETURN custom.multiFun(point({x: 1, y:1}), datetime('2020'), duration('P5M1DT12H')) as result",
                (r) -> assertEquals("P5M1DT12H, 1577836799999, point({x: 1.0, y: 1.0, crs: 'cartesian'})", r.get("result")));
    }

    @Test
    public void  testinstallFunctionReturnTypes() {
        // given
        db.executeTransactionally("UNWIND range(1, 4) as val CREATE (i:Target {value: val});");

        // when
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('ret_node(val :: INTEGER) :: NODE ', 'MATCH (t:Target {value : $val}) RETURN t')");
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('ret_node_list(val :: INTEGER) :: LIST OF NODE ', 'MATCH (t:Target {value : $val}) RETURN [t]')");
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('ret_map(val :: INTEGER) :: MAP ', 'RETURN {value : $val} as value')");
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('ret_map_list(val :: INTEGER) :: LIST OF MAP ', 'RETURN [{value : $val}] as value')");
        testCallCountEventually(db, "CALL apoc.custom.list", 4, TIMEOUT);

        // then
        testCallEventually("RETURN custom.ret_node(1) AS val", (result) -> {
            Node node = (Node) result.get("val");
            assertTrue(node.hasLabel(Label.label("Target")));
            assertEquals(1L, node.getProperty("value"));
        });
        testCallEventually("RETURN custom.ret_node_list(2) AS val", (result) -> {
            List<List<Node>> nodes = (List<List<Node>>) result.get("val");
            assertEquals(1, nodes.size());
            Node node = nodes.get(0).get(0);
            assertTrue(node.hasLabel(Label.label("Target")));
            assertEquals(2L, node.getProperty("value"));
        });
        testCallEventually("RETURN custom.ret_map(3) AS val", (result) -> {
            Map<String, Map> map = (Map<String, Map>) result.get("val");
            assertEquals(1, map.size());
            assertEquals(3L, map.get("value").get("value"));
        });
        testCallEventually("RETURN custom.ret_map_list(4) AS val", (result) -> {
            List<Map<String, List<Map>>> list = (List<Map<String, List<Map>>>) result.get("val");
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).size());
            assertEquals(4L, list.get(0).get("value").get(0).get("value"));
        });
    }

    @Test
    public void registerSimpleStatementFunction() throws Exception {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('answer2() :: STRING','RETURN 42 as answer')");
        testCallEventually("return custom.answer2() as row", (row) -> assertEquals(42L, row.get("row")));
    }

    @Test
    public void registerSimpleStatementFunctionWithOneChar() {
        final String procedureSignature = "b() :: STRING";
        assertProcedureFails(sysDb, String.format(SIGNATURE_SYNTAX_ERROR, procedureSignature),
                "CALL apoc.custom.installFunction('" + procedureSignature + "','RETURN 42 as answer')");
    }

    @Test
    public void shouldOverrideAndRemoveTheCustomFunctionWithDotInName() {
        // given
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('foo.bar() :: INT', 'RETURN 42 AS answer')");
        testCallEventually("return custom.foo.bar() as row", (row) -> assertEquals(42L, row.get("row")));
        db.executeTransactionally("call db.clearQueryCaches()");
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('foo.bar() :: INT', 'RETURN 43 AS answer')");
        testCallEventually("return custom.foo.bar() as row", (row) -> assertEquals(43L, row.get("row")));
        db.executeTransactionally("call db.clearQueryCaches()");

        // when
        sysDb.executeTransactionally("call apoc.custom.dropFunction('foo.bar')");
        db.executeTransactionally("call db.clearQueryCaches()");

        // then
        testProcFunFailEventually("RETURN custom.foo.bar()",
                "Unknown function 'custom.foo.bar'");
        testCallCount(db, "CALL apoc.custom.list", 0);

    }

    @Test
    public void shouldOverrideAndRemoveTheCustomProcedureWithDotInName() {
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('aa.bb.ccc() :: (answer::INT)','RETURN 42 as answer')");
        testCallEventually("call custom.aa.bb.ccc()", (row) -> assertEquals(42L, row.get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('aa.bb.ccc() :: (answer::INT)','RETURN 43 as answer')");
        testCallEventually("call custom.aa.bb.ccc()", (row) -> assertEquals(43L, row.get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        sysDb.executeTransactionally("call apoc.custom.dropProcedure('aa.bb.ccc')");
        db.executeTransactionally("call db.clearQueryCaches()");

        // then
        String query = "call custom.aa.bb.ccc()";
        String expectedErr = "There is no procedure with the name `custom.aa.bb.ccc` registered for this database instance. " +
                             "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.";
        testProcFunFailEventually(query, expectedErr);
        testCallCount(db, "CALL apoc.custom.list", 0);
    }

    @Test
    public void shouldRemoveTheCustomFunction() throws Exception {

        // given
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('aa.bb.ccc() :: INTEGER','RETURN 42 as answer')");
        testCallEventually("return custom.aa.bb.ccc() as row", (row) -> assertEquals(42L, row.get("row")));

        // when
        sysDb.executeTransactionally("call apoc.custom.dropFunction('aa.bb.ccc')");
        db.executeTransactionally("call db.clearQueryCaches()");

        // then
        String query = "RETURN custom.aa.bb.ccc()";
        String expectedErr = "Unknown function 'custom.aa.bb.ccc'";
        testProcFunFailEventually(query, expectedErr);
        testCallCount(db, "CALL apoc.custom.list", 0);
    }

    @Test
    public void shouldRemoveTheCustomProcedure(){
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('aa.bb.ccc(input :: NUMBER) :: (answer::INT)','RETURN 42 as answer')");
        testCallEventually("call custom.aa.bb.ccc(42)", (row) -> assertEquals(42L, row.get("answer")));

        // when
        sysDb.executeTransactionally("call apoc.custom.dropProcedure('aa.bb.ccc')");
        db.executeTransactionally("call db.clearQueryCaches()");

        String query = "call custom.aa.bb.ccc(222)";
        String expectedErr = "There is no procedure with the name `custom.aa.bb.ccc` registered for this database instance. " +
                             "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.";
        testProcFunFailEventually(query, expectedErr);
        testCallCount(db, "CALL apoc.custom.list", 0);
    }

    @Test
    public void shouldOverrideCustomFunctionWithDotInNameOnlyIfWithSameNamespaceAndFinalName() throws Exception {

        // given
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('aa.bb.name() :: NUMBER','RETURN 42 as answer')");
        testCallEventually("return custom.aa.bb.name() as answer", (row) -> assertEquals(42L, row.get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('aa.bb.name() :: NUMBER','RETURN 34 as answer')");
        testCallEventually("return custom.aa.bb.name() as answer", (row) -> assertEquals(34L, row.get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('aa.bb.name() :: NUMBER','RETURN 12 as answer')");
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('xx.yy.name() :: NUMBER','RETURN 44 as answer')");
        testCallEventually("return custom.aa.bb.name() as answer", (row) -> assertEquals(12L, row.get("answer")));
        testCallEventually("return custom.xx.yy.name() as answer", (row) -> assertEquals(44L, row.get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        testResult(db, "call apoc.custom.list() YIELD name, statement RETURN name, statement ORDER BY name", (row) -> {
            assertTrue(row.hasNext());
            Map<String, Object> mapFirst = row.next();
            assertEquals("aa.bb.name", mapFirst.get("name"));
            assertEquals("RETURN 12 as answer", mapFirst.get("statement"));
            assertTrue(row.hasNext());
            Map<String, Object> mapSecond = row.next();
            assertEquals("xx.yy.name", mapSecond.get("name"));
            assertEquals("RETURN 44 as answer", mapSecond.get("statement"));
            assertFalse(row.hasNext());
        });

        sysDb.executeTransactionally("call apoc.custom.dropFunction('aa.bb.name')");
        db.executeTransactionally("call db.clearQueryCaches()");

        testCallEventually("call apoc.custom.list", (row) -> {
            assertEquals("xx.yy.name", row.get("name"));
            assertEquals("RETURN 44 as answer", row.get("statement"));
            assertEquals(FUNCTION, row.get("type"));
        });

        sysDb.executeTransactionally("call apoc.custom.dropFunction('xx.yy.name')");
        db.executeTransactionally("call db.clearQueryCaches()");

        testCallCountEventually(db, "call apoc.custom.list()", 0, TIMEOUT);
    }

    @Test
    public void shouldOverrideCustomProcedureWithDotInNameOnlyIfWithSameNamespaceAndFinalName() {

        // given
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('xx.zz.name() :: (answer::INT)','RETURN 42 as answer')");
        testCallEventually("call custom.xx.zz.name()", (row) -> assertEquals(42L, row.get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('xx.zz.name() :: (answer::INT)','RETURN 34 as answer')");
        testCallEventually("call custom.xx.zz.name()", (row) -> assertEquals(34L, row.get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('aa.bb.name() :: (answer::INT)','RETURN 12 as answer')");
        testCallEventually("call custom.aa.bb.name()", (row) -> assertEquals(12L, row.get("answer")));
        testCallEventually("call custom.xx.zz.name()", (row) -> assertEquals(34L, row.get("answer")));

        testResult(db, "call apoc.custom.list() YIELD name, statement, type RETURN * ORDER BY name",
                (row) -> {
            assertTrue(row.hasNext());
            Map<String, Object> mapFirst = row.next();
            assertEquals("aa.bb.name", mapFirst.get("name"));
            assertEquals("RETURN 12 as answer", mapFirst.get("statement"));
            assertEquals(PROCEDURE, mapFirst.get("type"));
            assertTrue(row.hasNext());
            Map<String, Object> mapSecond = row.next();
            assertEquals("xx.zz.name", mapSecond.get("name"));
            assertEquals("RETURN 34 as answer", mapSecond.get("statement"));
            assertEquals(PROCEDURE, mapSecond.get("type"));
            assertFalse(row.hasNext());
        });

        sysDb.executeTransactionally("call apoc.custom.dropProcedure('aa.bb.name')");
        db.executeTransactionally("call db.clearQueryCaches()");

        testCallEventually("call apoc.custom.list", (row) -> {
            assertEquals("xx.zz.name", row.get("name"));
            assertEquals("RETURN 34 as answer", row.get("statement"));
            assertEquals(PROCEDURE, row.get("type"));
        });

        sysDb.executeTransactionally("call apoc.custom.dropProcedure('xx.zz.name')");
        db.executeTransactionally("call db.clearQueryCaches()");

        testCallCountEventually(db, "call apoc.custom.list()", 0, TIMEOUT);
    }

    @Test
    public void shouldRemovalOfProcedureNodeDeactivate() {
        //given
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('answer() :: (answer::ANY)','RETURN 42 AS answer')");
        testCallEventually("call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));

        // remove the node in systemdb
        GraphDatabaseService systemDb = databaseManagementService.database("system");
        try (Transaction tx = systemDb.beginTx()) {
            Node node = tx.findNode(ApocCypherProcedures, SystemPropertyKeys.name.name(), "answer");
            node.delete();
            tx.commit();
        }

        // refresh procedures
        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(RegisterComponentFactory.RegisterComponentLifecycle.class);
        CypherProceduresHandler cypherProceduresHandler = (CypherProceduresHandler) registerComponentLifecycle.getResolvers().get(CypherProceduresHandler.class).get(db.databaseName());
        cypherProceduresHandler.restoreProceduresAndFunctions();

        // when
        String query = "call custom.answer()";
        String expectedErr = "There is no procedure with the name `custom.answer` registered for this database instance. " +
                             "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.";
        testProcFunFailEventually(query, expectedErr);
    }

    @Test
    public void shouldRemovalOfFunctionNodeDeactivate() {

        //given
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('answer() :: INTEGER','RETURN 42')");
        testCallEventually("return custom.answer() as row", (row) -> assertEquals(42L, row.get("row")));

        // remove the node in systemdb
        GraphDatabaseService systemDb = databaseManagementService.database("system");
        try (Transaction tx = systemDb.beginTx()) {
            Node node = tx.findNode(ApocCypherProcedures, SystemPropertyKeys.name.name(), "answer");
            node.delete();
            tx.commit();
        }

        // refresh procedures
        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(RegisterComponentFactory.RegisterComponentLifecycle.class);
        CypherProceduresHandler cypherProceduresHandler = (CypherProceduresHandler) registerComponentLifecycle.getResolvers().get(CypherProceduresHandler.class).get(db.databaseName());
        cypherProceduresHandler.restoreProceduresAndFunctions();

        // when
        testProcFunFailEventually("RETURN custom.answer()",
                "Unknown function 'custom.answer'");
    }

    @Test
    public void testIssue2605() {
        db.executeTransactionally("CREATE (n:Test {id: 1})-[:has]->(:Log), (n)-[:has]->(:System)");
        String query = "MATCH (node:Test)-[:has]->(log:Log) WHERE node.id = $id WITH node \n" +
                "MATCH (node)-[:has]->(log:System) RETURN log, node";
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('testIssue2605(id :: INTEGER ) :: (log :: NODE, node :: NODE)', $query, 'neo4j' ,'read')", Map.of("query", query));

        // check query
        testCallEventually("call custom.testIssue2605(1)", (row) -> {
            assertEquals(List.of(Label.label("Test")), ((Node) row.get("node")).getLabels());
            assertEquals(List.of(Label.label("System")), ((Node) row.get("log")).getLabels());
        });

        // UNION ALL github issue case
        db.executeTransactionally("CREATE (n:ExampleNode {id: 1}), (:OtherExampleNode {identifier: '1'})");
        String query2 = "MATCH (:ExampleNode)\n" +
                " OPTIONAL MATCH (o:OtherExampleNode {identifier:$exampleId})\n" +
                " RETURN o.identifier as value\n" +
                " UNION ALL\n" +
                " MATCH (n:ExampleNode)\n" +
                " OPTIONAL MATCH (o:OtherExampleNode {identifier:$exampleId})\n" +
                " RETURN o.identifier as value";
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('exampleTest(exampleId::STRING) ::(value::STRING)', $query, 'neo4j', 'read')", Map.of("query", query2));

        // check query
        final String identifier = "1";
        testCallEventually("call custom.exampleTest($id) YIELD value RETURN collect(value) AS values", Map.of("id", identifier),
                (r) -> {
                    List<String> expected = List.of(identifier, identifier);
                    assertEquals(expected, r.get("values"));
                });
    }

    @Test
    public void shouldinstallProcedureWithDefaultBooleanOrNull() {
        sysDb.executeTransactionally("call apoc.custom.installProcedure('procWithBool(minScore = true :: BOOLEAN) :: (res :: INT)',\n" +
                "    'RETURN case when $minScore then 1 else 2 end as res')");


        testCallEventually("call custom.procWithBool", (row) -> assertEquals(1L, row.get("res")));
        testCallEventually("call custom.procWithBool(true)", (row) -> assertEquals(1L, row.get("res")));
        testCallEventually("call custom.procWithBool(false)", (row) -> assertEquals(2L, row.get("res")));

        sysDb.executeTransactionally("call apoc.custom.installProcedure('procWithNull(minScore = null :: INT) :: (res :: INT)',\n" +
                "    'RETURN $minScore as res')");
        testCallEventually("call custom.procWithNull", (row) -> assertNull(row.get("res")));
        testCallEventually("call custom.procWithNull(1)", (row) -> assertEquals(1L, row.get("res")));
    }

    @Test
    public void shouldinstallFunctionWithDefaultBooleanOrNull() {
        sysDb.executeTransactionally("call apoc.custom.installFunction('funWithBool(minScore = true :: BOOLEAN) :: INT',\n" +
                "'RETURN case when $minScore then 1 else 2 end as res')");
        testCallEventually("RETURN custom.funWithBool() AS res", (row) -> assertEquals(1L, row.get("res")));
        testCallEventually("RETURN custom.funWithBool(true) AS res", (row) -> assertEquals(1L, row.get("res")));
        testCallEventually("RETURN custom.funWithBool(false) AS res", (row) -> assertEquals(2L, row.get("res")));

        sysDb.executeTransactionally("call apoc.custom.installFunction('funWithNull(minScore = null :: INT) :: INT',\n" +
                "'RETURN $minScore as res')");
        testCallEventually("RETURN custom.funWithNull() AS res", (row) -> assertNull(row.get("res")));
        testCallEventually("RETURN custom.funWithNull(1) AS res", (row) -> assertEquals(1L, row.get("res")));
    }

    @Test
    public void shouldFailinstallFunctionWithDefaultNumberParameters() {
        final String query = "RETURN $base * $exp AS res";
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('defaultFloatFun(base=2.4::FLOAT,exp=1.2::FLOAT):: INT', $query)",
                Map.of("query", query));
        testCallEventually("RETURN custom.defaultFloatFun() AS res", (row) -> assertEquals(2.4D * 1.2D, (double) row.get("res"), 0.1D));
        testCallEventually("RETURN custom.defaultFloatFun(1.1) AS res", (row) -> assertEquals(1.1D * 1.2D, (double) row.get("res"), 0.1D));
        testCallEventually("RETURN custom.defaultFloatFun(1.5, 7.1) AS res", (row) -> assertEquals(1.5D * 7.1D, (double) row.get("res"), 0.1D));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('defaultDoubleFun(base = 2.4 :: DOUBLE, exp = 1.2 :: DOUBLE):: DOUBLE', $query)",
                Map.of("query", query));
        testCallEventually("RETURN custom.defaultDoubleFun() AS res", (row) -> assertEquals(2.4D * 1.2D, (double) row.get("res"), 0.1D));
        testCallEventually("RETURN custom.defaultDoubleFun(1.1) AS res", (row) -> assertEquals(1.1D * 1.2D, (double) row.get("res"), 0.1D));
        testCallEventually("RETURN custom.defaultDoubleFun(1.5, 7.1) AS res", (row) -> assertEquals(1.5D * 7.1D, (double) row.get("res"), 0.1D));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('defaultIntFun(base = 4 ::INT, exp = 5 :: INT):: INT', $query)",
                Map.of("query", query));
        testCallEventually("RETURN custom.defaultIntFun() AS res", (row) -> assertEquals(4L * 5L, row.get("res")));
        testCallEventually("RETURN custom.defaultIntFun(2) AS res", (row) -> assertEquals(2L * 5L, row.get("res")));
        testCallEventually("RETURN custom.defaultIntFun(3, 7) AS res", (row) -> assertEquals(3L * 7L, row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('defaultLongFun(base = 4 ::LONG, exp = 5 :: LONG):: LONG', $query)",
                Map.of("query", query));
        testCallEventually("RETURN custom.defaultLongFun() AS res", (row) -> assertEquals(4L * 5L, row.get("res")));
        testCallEventually("RETURN custom.defaultLongFun(2) AS res", (row) -> assertEquals(2L * 5L, row.get("res")));
        testCallEventually("RETURN custom.defaultLongFun(3, 7) AS res", (row) -> assertEquals(3L * 7L, row.get("res")));
    }

    @Test
    public void shouldFailinstallProcedureWithDefaultNumberParameters() {
        final String query = "RETURN $base * $exp AS res";
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('defaultFloatProc(base=2.4::FLOAT,exp=1.2::FLOAT)::(res::INT)', $query)",
                Map.of("query", query));

        testCallEventually("CALL custom.defaultFloatProc", (row) -> assertEquals(2.4D * 1.2D, (double) row.get("res"), 0.1D));
        testCallEventually("CALL custom.defaultFloatProc(1.1)", (row) -> assertEquals(1.1D * 1.2D, (double) row.get("res"), 0.1D));
        testCallEventually("CALL custom.defaultFloatProc(1.5, 7.1)", (row) -> assertEquals(1.5D * 7.1D, (double) row.get("res"), 0.1D));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('defaultDoubleProc(base = 2.4 :: DOUBLE, exp = 1.2 :: DOUBLE)::(res::DOUBLE)', $query)",
                Map.of("query", query));

        testCallEventually("CALL custom.defaultDoubleProc", (row) -> assertEquals(2.4D * 1.2D, (double) row.get("res"), 0.1D));
        testCallEventually("CALL custom.defaultDoubleProc(1.1)", (row) -> assertEquals(1.1D * 1.2D, (double) row.get("res"), 0.1D));
        testCallEventually("CALL custom.defaultDoubleProc(1.5, 7.1)", (row) -> assertEquals(1.5D * 7.1D, (double) row.get("res"), 0.1D));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('defaultIntProc(base = 4 ::INT, exp = 5 :: INT)::(res::INT)', $query)",
                Map.of("query", query));

        testCallEventually("CALL custom.defaultIntProc", (row) -> assertEquals(4L * 5L, row.get("res")));
        testCallEventually("CALL custom.defaultIntProc(2)", (row) -> assertEquals(2L * 5L, row.get("res")));
        testCallEventually("CALL custom.defaultIntProc(3, 7)", (row) -> assertEquals(3L * 7L, row.get("res")));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('defaultLongProc(base = 4 ::LONG, exp = 5 :: LONG)::(res::LONG)', $query)",
                Map.of("query", query));

        testCallEventually("CALL custom.defaultLongProc", (row) -> assertEquals(4L * 5L, row.get("res")));
        testCallEventually("CALL custom.defaultLongProc(2)", (row) -> assertEquals(2L * 5L, row.get("res")));
        testCallEventually("CALL custom.defaultLongProc(3, 7)", (row) -> assertEquals(3L * 7L, row.get("res")));
    }

    @Test
    public void shouldFailinstallFunctionAndProcedureWithInvalidParameterTypes() {
        final String procedureStatementInvalidInput = "sum(input:: INVALID) :: (answer::INT)";
        assertProcedureFails(sysDb, String.format(SIGNATURE_SYNTAX_ERROR, procedureStatementInvalidInput),
                "call apoc.custom.installProcedure('" + procedureStatementInvalidInput + "','RETURN $input AS input')");
        final String functionStatementInvalidInput = "double(input :: INVALID) :: INT";
        assertProcedureFails(sysDb, String.format(SIGNATURE_SYNTAX_ERROR, functionStatementInvalidInput),
                "call apoc.custom.installFunction('" + functionStatementInvalidInput + "','RETURN $input*2 as answer')");

        final String procedureStatementInvalidOutput = "myProc(input :: INTEGER) :: (sum :: DUNNO)";
        assertProcedureFails(sysDb, String.format(SIGNATURE_SYNTAX_ERROR, procedureStatementInvalidOutput),
                "call apoc.custom.installProcedure('" + procedureStatementInvalidOutput + "','RETURN $input AS sum')");
        final String functionStatementInvalidOutput = "myFunc(val :: INTEGER) :: DUNNO";
        assertProcedureFails(sysDb, String.format(SIGNATURE_SYNTAX_ERROR, functionStatementInvalidOutput),
                "CALL apoc.custom.installFunction('" + functionStatementInvalidOutput + "', 'RETURN $val')");
    }

    @Test
    public void shouldCreateFunctionWithDefaultParameters() {
        // default inputs
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('multiParDeclareFun(params = {} :: MAP) :: INT ', 'RETURN $one + $two as sum')");
        testCallEventually("return custom.multiParDeclareFun({one:2, two: 3}) as row", (row) -> assertEquals(5L, row.get("row")));

        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('multiParDeclareProc(params = {} :: MAP) :: (sum :: INT) ', 'RETURN $one + $two + $three as sum')");
        testCallEventually("call custom.multiParDeclareProc({one:2, two: 3, three: 4})", (row) -> assertEquals(9L, row.get("sum")));

        // default outputs
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('declareDefaultOut(one :: INTEGER, two :: INTEGER) :: (row :: MAP) ', 'RETURN $one + $two as sum')");
        testCallEventually("call custom.declareDefaultOut(5, 3)", (row) -> assertEquals(8L, ((Map<String, Object>)row.get("row")).get("sum")));
    }

    @Test
    public void testIssue2032() {
        String functionSignature = "foobar(xx::NODE, y::NODE) ::(NODE)";
        assertProcedureFails(sysDb, String.format(SIGNATURE_SYNTAX_ERROR, functionSignature),
                "CALL apoc.custom.installFunction('" + functionSignature + "', 'MATCH (n) RETURN n limit 1');");

        String procedureSignature = "testFail(first::INT, s::INT) :: (answer::INT)";
        assertProcedureFails(sysDb, String.format(SIGNATURE_SYNTAX_ERROR, procedureSignature),
                "call apoc.custom.installProcedure('" + procedureSignature + "','RETURN $first + $s AS answer')");
    }

    @Test
    public void testIssue3349() {
        String procedure = """
                CALL apoc.custom.installProcedure(
                  'retFunctionNames() :: (name :: STRING)',
                  'CALL dbms.listConfig() YIELD name RETURN name ',
                  'neo4j',
                  'DBMS'
                );""";
        sysDb.executeTransactionally(procedure);
        assertEventually(() -> {
            try {
                List<String> functions = db.executeTransactionally("CALL custom.retFunctionNames()", Map.of(), r -> r
                        .stream()
                        .map(m -> (String) m.get("name"))
                        .collect(Collectors.toList()));
                return !functions.isEmpty();
            } catch (Exception e) {
                return false;
            }
        }, (v) -> v, TIMEOUT, TimeUnit.SECONDS);

        String procedureVoid = """
                CALL apoc.custom.installProcedure(
                  'setTxMetadata(meta :: MAP) :: VOID',
                  'CALL tx.setMetaData($meta)',
                  'neo4j',
                  'DBMS'
                );""";
        sysDb.executeTransactionally(procedureVoid);
        // This should run without exception
        assertEventually(() -> {
            try {
                db.executeTransactionally("CALL custom.setTxMetadata($meta)", Map.of(
                        "meta", Map.of("foo", "bar")
                ));
                return true;
            } catch (Exception e) {
                return false;
            }
        }, (v) -> v, TIMEOUT, TimeUnit.SECONDS);

    }


    //
    // new test cases
    //

    @Test
    public void testCustomShow() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('my.fun() :: INTEGER','RETURN 42 as answer')");
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('my.proc() :: (answer::INT)','RETURN 42 as answer')");

        testCallEventually("return custom.my.fun() as row", (row) -> assertEquals(42L, row.get("row")));
        testCallEventually("call custom.my.proc()", (row) -> assertEquals(42L, row.get("answer")));

        testResult(sysDb, "CALL apoc.custom.show() YIELD name RETURN name ORDER BY name",
                (result) -> {
                    // then
                    Map<String, Object> row = result.next();
                    assertEquals("my.fun", row.get("name"));
                    row = result.next();
                    assertEquals("my.proc", row.get("name"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testShowCustomInUserDb() {
        try {
            testCall(db, "CALL apoc.custom.show()",
                    r -> fail("Should fail because of user db execution"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(NON_SYS_DB_ERROR));
        }
    }

    @Test
    public void testDropAllCustoms() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('my.fun() :: INTEGER','RETURN 42 as answer')");
        sysDb.executeTransactionally("CALL apoc.custom.installProcedure('my.proc() :: (answer::INT)','RETURN 42 as answer')");

        testCallEventually("return custom.my.fun() as row", (row) -> assertEquals(42L, row.get("row")));
        testCallEventually("call custom.my.proc()", (row) -> assertEquals(42L, row.get("answer")));
        testCallCountEventually(db, "CALL apoc.custom.list", 2, TIMEOUT);

        // when
        testResult(sysDb, "CALL apoc.custom.dropAll() YIELD name RETURN name ORDER BY name",
                (result) -> {
                    // then
                    Map<String, Object> row = result.next();
                    assertEquals("my.fun", row.get("name"));
                    row = result.next();
                    assertEquals("my.proc", row.get("name"));
                    assertFalse(result.hasNext());
                });

        String queryProc = "CALL custom.my.proc()";
        String expectedErrProc = "There is no procedure with the name `custom.my.proc` registered";
        testProcFunFailEventually(queryProc, expectedErrProc);

        String queryFun = "RETURN custom.my.fun()";
        String expectedErrFun = "Unknown function 'custom.my.fun'";
        testProcFunFailEventually(queryFun, expectedErrFun);
    }

    @Test
    public void testInstallCustomInUserDb() {
        assertProcedureFails(db, PROCEDURE_NOT_ROUTED_ERROR,
                "CALL apoc.custom.installFunction('my.fun() :: INTEGER','RETURN 42 as answer')");

        assertProcedureFails(db, PROCEDURE_NOT_ROUTED_ERROR,
                "CALL apoc.custom.installProcedure('my.proc() :: (answer::INT)','RETURN 42 as answer')");
    }

    @Test
    public void testInstallCustomInWrongDb() {
        String dbNotExistent = "notExistent";
        String expected = String.format("The user database with name '%s' does not exist", dbNotExistent);

        assertProcedureFails(sysDb, expected,
                "CALL apoc.custom.installFunction('my.fun() :: INTEGER','RETURN 42 as answer', $db)",
                Map.of("db", dbNotExistent));

        assertProcedureFails(sysDb, expected,
                "CALL apoc.custom.installFunction('my.fun() :: INTEGER','RETURN 42 as answer', $db)",
                Map.of("db", dbNotExistent));
    }

    @Test
    public void testInstallCustomInSystemDb() {
        assertProcedureFails(sysDb, BAD_TARGET_ERROR,
                "CALL apoc.custom.installFunction('my.fun() :: INTEGER','RETURN 42 as answer', 'system')");

        assertProcedureFails(sysDb, BAD_TARGET_ERROR,
                "CALL apoc.custom.installFunction('my.fun() :: INTEGER','RETURN 42 as answer', 'system')");
    }

    private void testCallEventually(String call, Consumer<Map<String, Object>> consumer) {
        testCallEventually(call, Map.of(), consumer);
    }

    private void testCallEventually(String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer) {
        testRetryCallEventually(db, call, params, consumer, TIMEOUT);
    }

    private void testProcFunFailEventually(String query, String expectedErr) {
        try {
            TestUtil.testCallCountEventually(db, query, 0, TIMEOUT);
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Actual error is: " + message, message.contains(expectedErr));
        }
    }

}