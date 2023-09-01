/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.custom;

import apoc.path.PathExplorer;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static apoc.custom.CypherProceduresHandler.CUSTOM_PROCEDURES_REFRESH;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProceduresStorageTest {
    private final static String QUERY_CREATE = "RETURN $input1 + $input2 as answer";
    private final static String QUERY_OVERWRITE = "RETURN $input1 + $input2 + 123 as answer";
    private final int refreshTime = 3000;
    private int greaterThanRefreshTime;

    @Rule
    public TemporaryFolder STORE_DIR = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;

    // start db with config: `apoc.custom.procedures.refresh=<time>`
    @Rule
    public final ProvideSystemProperty systemPropertyRule =
            new ProvideSystemProperty(CUSTOM_PROCEDURES_REFRESH, String.valueOf(refreshTime));

    @Before
    public void setUp() throws Exception {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot().toPath())
                .setConfig(procedure_unrestricted, List.of("apoc*"))
                .build();
        greaterThanRefreshTime = refreshTime + 500;

        db = databaseManagementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, CypherProcedures.class, PathExplorer.class);
    }

    @After
    public void teardown() {
        databaseManagementService.shutdown();
    }

    private void restartDb() {
        databaseManagementService.shutdown();
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000));
        TestUtil.registerProcedure(db, CypherProcedures.class, PathExplorer.class);
    }

    @Test
    public void overloadFunctionAfterRefresh() throws Exception {
        db.executeTransactionally("CALL apoc.custom.declareFunction('overloadFun() :: LONG','RETURN 10')");

        TestUtil.testCall(db, "RETURN custom.overloadFun() AS result", r -> {
            assertEquals(10L, r.get("result"));
        });

        try {
            TestUtil.testCall(db, "RETURN custom.overloadFun(42)",
                    r -> fail("Should fail due to wrong argument numbers"));
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Current message is: " + message,
                    message.contains("Function call does not provide the required number of arguments: expected 0 got 1"));
        }

        db.executeTransactionally("CALL apoc.custom.declareFunction('overloadFun(input::LONG) :: LONG', 'RETURN $input')");

        // check overload before refresh
        checkFunctionOverloaded();

        // wait a time greater then the `apoc.custom.procedures.refresh` value
        // and check overload works correctly

        Thread.sleep(greaterThanRefreshTime);
        checkFunctionOverloaded();

        // check overload still remains after restarting the db
        restartDb();
        checkFunctionOverloaded();

        // overload with a function having an optional string argument
        db.executeTransactionally("CALL apoc.custom.declareFunction('overloadFun(input = null :: STRING) :: STRING', 'RETURN $input')");

        // we test the new function like above
        checkSecondFunctionOverloaded();

        Thread.sleep(greaterThanRefreshTime);
        checkSecondFunctionOverloaded();

        restartDb();
        checkSecondFunctionOverloaded();
    }

    private void checkFunctionOverloaded() {
        db.executeTransactionally("call db.clearQueryCaches()");

        TestUtil.testCall(db, "RETURN custom.overloadFun(42) AS result", r -> {
            assertEquals(42L, r.get("result"));
        });

        try {
            TestUtil.testCall(db, "RETURN custom.overloadFun()",
                    r -> fail("Should fail due to wrong argument numbers"));
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Current message is: " + message,
                    message.contains("Function call does not provide the required number of arguments: expected 1 got 0"));
        }
    }

    private void checkSecondFunctionOverloaded() {
        db.executeTransactionally("call db.clearQueryCaches()");

        TestUtil.testCall(db, "RETURN custom.overloadFun('42') AS result", r -> {
            assertEquals("42", r.get("result"));
        });

        TestUtil.testCall(db, "RETURN custom.overloadFun() AS result", r -> {
            assertNull(r.get("result"));
        });

        try {
            TestUtil.testCall(db, "RETURN custom.overloadFun(42) AS result",
                    r -> fail("Should fail due to wrong argument numbers"));
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Current message is: " + message,
                    message.contains("Type mismatch: expected String but was Integer"));
        }
    }

    @Test
    public void overloadProcedureAfterRefresh() throws Exception {
        db.executeTransactionally("CALL apoc.custom.declareProcedure('overloadProc() :: (result::LONG)','RETURN 10 as result')");

        TestUtil.testCall(db, "CALL custom.overloadProc()", r -> {
            assertEquals(10L, r.get("result"));
        });

        try {
            TestUtil.testCall(db, "CALL custom.overloadProc(42)",
                    r -> fail("Should fail due to wrong argument numbers"));
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Current message is: " + message,
                    message.contains("Procedure call provides too many arguments: got 1 expected none"));
        }

        db.executeTransactionally("CALL apoc.custom.declareProcedure('overloadProc(input::LONG) :: (result::LONG)', 'RETURN $input AS result')");

        // check overload before refresh
        checkProcedureOverloaded();

        // wait a time greater than `apoc.custom.procedures.refresh` value
        // and check overload works correctly
        Thread.sleep(greaterThanRefreshTime);
        checkProcedureOverloaded();

        // check overload still remains after restarting the db
        restartDb();
        checkProcedureOverloaded();

        // overload with a procedure having an optional string argument
        db.executeTransactionally("CALL apoc.custom.declareProcedure('overloadProc(input = \"def\" :: STRING) :: (result::STRING)', 'RETURN $input AS result')");

        // we test the new procedure like above
        checkSecondProcedureOverloaded();

        Thread.sleep(greaterThanRefreshTime);
        checkSecondProcedureOverloaded();

        restartDb();
        checkSecondProcedureOverloaded();
    }

    private void checkProcedureOverloaded() {
        db.executeTransactionally("call db.clearQueryCaches()");

        try {
            TestUtil.testCall(db, "CALL custom.overloadProc()",
                    r -> fail("Should fail due to wrong argument numbers"));
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Current message is: " + message,
                    message.contains("Procedure call does not provide the required number of arguments: got 0 expected at least 1"));
        }

        TestUtil.testCall(db, "CALL custom.overloadProc(42)", r -> {
            assertEquals(42L, r.get("result"));
        });
    }

    private void checkSecondProcedureOverloaded() {
        db.executeTransactionally("call db.clearQueryCaches()");

        TestUtil.testCall(db, "CALL custom.overloadProc('42')", r -> {
            assertEquals("42", r.get("result"));
        });

        TestUtil.testCall(db, "CALL custom.overloadProc()", r -> {
            assertEquals("def", r.get("result"));
        });

        try {
            TestUtil.testCall(db, "CALL custom.overloadProc(42)",
                    r -> fail("Should fail due to wrong argument numbers"));
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Current message is: " + message,
                    message.contains("Type mismatch: expected String but was Integer"));
        }
    }

    @Test
    public void registerSimpleStatement() {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        restartDb();
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("answer", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
    }

    @Test
    public void registerSimpleFunctionWithDotInName() {
        db.executeTransactionally("call apoc.custom.asFunction('foo.bar.baz','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.foo.bar.baz() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
        restartDb();
        TestUtil.testCall(db, "return custom.foo.bar.baz() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
    }

    @Test
    public void registerSimpleProcedureWithDotInName() {
        db.executeTransactionally("call apoc.custom.asProcedure('foo.bar.baz','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.foo.bar.baz()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
        restartDb();
        TestUtil.testCall(db, "call custom.foo.bar.baz()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
    }

    @Test
    public void registerSimpleStatementConcreteResults() {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer','read',[['answer','long']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatement() {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $answer as answer')");
        restartDb();
        TestUtil.testCall(db, "call custom.answer({answer:42})", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterStatement() {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',null,[['input','number']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer(42)", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatement() {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypes() {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','read',null," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2}))", (row) -> assertEquals(9, ((List)((Map)row.get("row")).get("data")).size()));
    }

    @Test
    public void registerSimpleStatementFunction() {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        restartDb();
        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("answer", row.get("name"));
            assertEquals("function", row.get("type"));
        });
    }

    @Test
    public void registerSimpleStatementFunctionWithDotInName() {
        db.executeTransactionally("call apoc.custom.asFunction('foo.bar.baz','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.foo.bar.baz() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
        restartDb();
        TestUtil.testCall(db, "return custom.foo.bar.baz() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunction() {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42 as answer','long')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunctionUnnamedResultColumn() {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatementFunction() {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $answer as answer','long')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer({answer:42}) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatementFunction() {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $input as answer','long',[['input','number']])");
        restartDb();
        TestUtil.testCall(db, "return custom.answer(42) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypesFunction() {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','list of any'," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']], true)");
        restartDb();
        TestUtil.testCall(db, "return custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2})) as data", (row) -> assertEquals(9, ((List)row.get("data")).size()));
    }

    @Test
    public void testIssue1744() throws Exception {
        db.executeTransactionally("CREATE (:Area {name: 'foo'})-[:CURRENT]->(:VantagePoint {alpha: 'beta'})");
        db.executeTransactionally("CALL apoc.custom.asProcedure('vantagepoint_within_area',\n" +
            "  \"MATCH (start:Area {name: $areaName} )\n" +
            "    CALL apoc.path.expand(start,'CONTAINS>|<SEES|CURRENT','',0,100) YIELD path\n" +
            "    UNWIND nodes(path) as node\n" +
            "    WITH node\n" +
            "    WHERE node:VantagePoint\n" +
            "    RETURN DISTINCT node as resource\",\n" +
            "  'read',\n" +
            "  [['resource','NODE']],\n" +
            "  [['areaName', 'STRING']],\n" +
            "  \"Get vantage points within an area and all included areas\");");

        // function analogous to procedure
        db.executeTransactionally("CALL apoc.custom.asFunction('vantagepoint_within_area',\n" +
            "  \"MATCH (start:Area {name: $areaName} )\n" +
            "    CALL apoc.path.expand(start,'CONTAINS>|<SEES|CURRENT','',0,100) YIELD path\n" +
            "    UNWIND nodes(path) as node\n" +
            "    WITH node\n" +
            "    WHERE node:VantagePoint\n" +
            "    RETURN DISTINCT node as resource\",\n" +
            "  'read',\n" +
            "  [['areaName', 'STRING']]);");

        testCallIssue1744();
        restartDb();

        final String logFileContent = Files.readString(new File(FileUtils.getLogDirectory(), "debug.log").toPath());
        assertFalse(logFileContent.contains("Could not register function: custom.vantagepoint_within_area"));
        assertFalse(logFileContent.contains("Could not register procedure: custom.vantagepoint_within_area"));
        testCallIssue1744();
    }

    @Test
    public void testMultipleOverrideWithFunctionAndProcedures() throws Exception {
        db.executeTransactionally("CALL apoc.custom.declareProcedure('override() :: (result::LONG)','RETURN 42 as result')");

        // function homonym to procedure
        db.executeTransactionally("CALL apoc.custom.declareFunction('override() :: LONG','RETURN 10 as answer')");

        // get fun/proc created
        TestUtil.testCall(db, "RETURN custom.override() as result", r -> {
            assertEquals(10L, r.get("result"));
        });
        TestUtil.testCall(db, "CALL custom.override()", r -> {
            assertEquals(42L, r.get("result"));
        });

        // overrides functions and procedures homonym to the previous ones
        db.executeTransactionally("CALL apoc.custom.declareFunction('override(input::INT) :: INT', 'RETURN $input + 2 AS result')");
        db.executeTransactionally("CALL apoc.custom.declareFunction('override(input::INT) :: INT', 'RETURN $input AS result')");

        db.executeTransactionally("CALL apoc.custom.declareProcedure('override(input::INT) :: (result::INT)', 'RETURN $input AS result')");
        db.executeTransactionally("CALL apoc.custom.declareProcedure('override(input::INT) :: (result::INT)', 'RETURN $input + 2 AS result')");

        // get fun/proc updated
        TestUtil.testCallEventually(db, "RETURN custom.override(3) as result", r -> {
            assertEquals(3L, r.get("result"));
        }, 10L);
        TestUtil.testCallEventually(db, "CALL custom.override(2)", r -> {
            assertEquals(4L, r.get("result"));
        }, 10L);

        // check fun/proc updated work even after the refresh
        Thread.sleep(greaterThanRefreshTime);
        TestUtil.testCall(db, "RETURN custom.override(3) as result", r -> {
            assertEquals(3L, r.get("result"));
        });
        TestUtil.testCall(db, "CALL custom.override(2)", r -> {
            assertEquals(4L, r.get("result"));
        });

        restartDb();

        final String logFileContent = Files.readString(new File(FileUtils.getLogDirectory(), "debug.log").toPath());
        assertFalse(logFileContent.contains("Could not register function: custom.vantagepoint_within_area"));
        assertFalse(logFileContent.contains("Could not register procedure: custom.vantagepoint_within_area"));

        // override after restart
        db.executeTransactionally("CALL apoc.custom.declareFunction('override(input::INT) :: INT', 'RETURN $input + 1 AS result')");
        db.executeTransactionally("CALL apoc.custom.declareProcedure('override(input::INT) :: (result::INT)', 'RETURN $input + 2 AS result')");

        // get fun/proc updated
        TestUtil.testCallEventually(db, "RETURN custom.override(3) as result", r -> {
            assertEquals(4L, r.get("result"));
        }, 10L);
        TestUtil.testCallEventually(db, "CALL custom.override(2)", r -> {
            assertEquals(4L, r.get("result"));
        }, 10L);
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
        db.executeTransactionally("call apoc.custom.declareFunction('sumFun2(input1::INT, input2::INT) :: INT',$query)",
                map("query", QUERY_OVERWRITE));
        db.executeTransactionally("call apoc.custom.declareFunction('sumFun1(input1 = null::INT, input2 = null::INT) :: INT',$query)",
                map("query", QUERY_OVERWRITE));
        functionAssertions();
    }

    @Test
    public void procedureSignatureShouldNotChangeBeforeAndAfterRestartAndOverwrite() {
        proceduresCreation();

        procedureAssertions();
        restartDb();

        // overwrite function
        db.executeTransactionally("call apoc.custom.declareProcedure('sum1(input1 = null::INT, input2 = null::INT) :: (answer::INT)',$query)",
                map("query", QUERY_OVERWRITE));
        db.executeTransactionally("call apoc.custom.declareProcedure('sum2(input1::INT, input2::INT) :: (answer::INT)',$query)",
                map("query", QUERY_OVERWRITE));
        procedureAssertions();
    }

    private void functionsCreation() {
        // 1 declareFunction with default null, 1 declareFunction without default
        db.executeTransactionally("call apoc.custom.declareFunction('sumFun1(input1 = null::INT, input2 = null::INT) :: INT',$query)",
                map("query", QUERY_CREATE));
        db.executeTransactionally("call apoc.custom.declareFunction('sumFun2(input1::INT, input2::INT) :: INT',$query)",
                map("query", QUERY_CREATE));
    }

    private void functionAssertions() {
        TestUtil.testResult(db, "SHOW FUNCTIONS YIELD signature, name WHERE name STARTS WITH 'custom.sumFun' RETURN DISTINCT name, signature ORDER BY name",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals("custom.sumFun1(input1 = null :: INTEGER?, input2 = null :: INTEGER?) :: (INTEGER?)", row.get("signature"));
                    row = r.next();
                    assertEquals("custom.sumFun2(input1 :: INTEGER?, input2 :: INTEGER?) :: (INTEGER?)", row.get("signature"));
                    assertFalse(r.hasNext());
                });
        TestUtil.testResult(db, "call apoc.custom.list() YIELD name RETURN name ORDER BY name",
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
        db.executeTransactionally("call apoc.custom.declareProcedure('sum1(input1 = null::INT, input2 = null::INT) :: (answer::INT)',$query)",
                map("query", QUERY_CREATE));
        db.executeTransactionally("call apoc.custom.declareProcedure('sum2(input1::INT, input2::INT) :: (answer::INT)',$query)",
                map("query", QUERY_CREATE));
    }

    private void procedureAssertions() {
        TestUtil.testResult(db, "SHOW PROCEDURES YIELD signature, name WHERE name STARTS WITH 'custom.sum' RETURN DISTINCT name, signature ORDER BY name",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals("custom.sum1(input1 = null :: INTEGER?, input2 = null :: INTEGER?) :: (answer :: INTEGER?)", row.get("signature"));
                    row = r.next();
                    assertEquals("custom.sum2(input1 :: INTEGER?, input2 :: INTEGER?) :: (answer :: INTEGER?)", row.get("signature"));
                    assertFalse(r.hasNext());
                });
        TestUtil.testResult(db, "call apoc.custom.list() YIELD name RETURN name ORDER BY name",
                row -> {
                    final List<String> sumFun1 = List.of("sum1", "sum2");
                    assertEquals(sumFun1, Iterators.asList(row.columnAs("name")));
                });

        TestUtil.testCall(db, String.format("CALL %s", "custom.sum1"), (row) -> assertNull(row.get("answer")));
        try {
            TestUtil.testCall(db, String.format("CALL %s()", "custom.sum2"), (row) -> fail("Should fail because of missing params"));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Procedure call does not provide the required number of arguments: got 0 expected at least 2"));
        }
    }

    private void testCallIssue1744() {
        TestUtil.testCall(db, "CALL custom.vantagepoint_within_area('foo')", this::assertCallIssue1744);
        TestUtil.testCall(db, "RETURN custom.vantagepoint_within_area('foo') as resource", this::assertCallIssue1744);
    }

    private void assertCallIssue1744(Map<String, Object> row) {
        final Node resource = (Node) row.get("resource");
        assertEquals("VantagePoint", resource.getLabels().iterator().next().name());
        assertEquals("beta", resource.getProperty("alpha"));
    }

    @Test
    public void testIssue1714WithRestartDb() throws Exception {
        db.executeTransactionally("CREATE (i:Target {value: 2});");
        db.executeTransactionally("CALL apoc.custom.asFunction('n', 'MATCH (t:Target {value : $val}) RETURN t', 'NODE', [['val', 'INTEGER']])");
        restartDb();
        TestUtil.testCall(db, "RETURN custom.n(2) as row", (row) -> assertEquals(2L, ((Node) row.get("row")).getProperty("value")));
    }
}
