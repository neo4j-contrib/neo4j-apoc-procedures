package apoc.neo4j.docker;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.custom.CypherProceduresHandler.CUSTOM_PROCEDURES_REFRESH;
import static apoc.util.ExtendedTestContainerUtil.singleResultFirstColumn;
import static apoc.util.TestContainerUtil.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CustomNewProcedureMultiDbTest {

    public static final long TIMEOUT = 10L;
    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;
    private static Session neo4jSession;
    private static Session testSession;
    private static Session fooSession;
    private static Session systemSession;

    private static final String DB_TEST = "dbtest";
    private static final String DB_FOO = "dbfoo";
    private static final List<String> ALL_DB_NAMES = List.of(DB_TEST, DB_FOO, DEFAULT_DATABASE_NAME);

    @BeforeClass
    public static void setupContainer() {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED), true)
                .withNeo4jConfig(CUSTOM_PROCEDURES_REFRESH, "1000");
        neo4jContainer.start();
        driver = neo4jContainer.getDriver();
        createDatabases();
        createSessions();
    }

    @After
    public void cleanDb() {
        systemSession.executeWrite(tx -> {
            for (String db: ALL_DB_NAMES) {
                tx.run("CALL apoc.custom.dropAll('" + db + "')").consume();
            }
            tx.run("CALL apoc.custom.dropAll(null)").consume();
            return null;
        });
    }

    @AfterClass
    public static void bringDownContainer() {
        neo4jContainer.close();
    }
    
    @Test
    public void testProceduresFunctionsInMultipleDatabase() {

        // install a procedure and a function for each database
        installNeo4jProcAndFun();
        installFooProcAndFun();
        installTestProcAndFun();

        // check that every database has 2 custom procedures/functions
        String countCustom = "CALL apoc.custom.list() YIELD name RETURN count(*) AS count";
        assertEventually(() -> (long) singleResultFirstColumn(neo4jSession, countCustom),
                (value) -> value == 2L, TIMEOUT, SECONDS);
        
        assertEventually(() -> (long) singleResultFirstColumn(fooSession, countCustom),
                (value) -> value == 2L, TIMEOUT, SECONDS);
        
        assertEventually(() -> (long) singleResultFirstColumn(testSession, countCustom),
                (value) -> value == 2L, TIMEOUT, SECONDS);

        // clear caches
        neo4jSession.executeWrite(tx->tx.run("call db.clearQueryCaches()").consume());
        fooSession.executeWrite(tx->tx.run("call db.clearQueryCaches()").consume());
        testSession.executeWrite(tx->tx.run("call db.clearQueryCaches()").consume());
        
        // check that each procedure and function is installed only in the specified db
        chackThatFunAndProcAreInstalledOnlyInTheSpecifiedDb(neo4jSession, 
                "CALL custom.neo4jProc", "RETURN custom.neo4jFun() AS answer",
                fooSession, testSession);

        chackThatFunAndProcAreInstalledOnlyInTheSpecifiedDb(fooSession, 
                "CALL custom.fooProc", "RETURN custom.fooFun() AS answer",
                neo4jSession, testSession);
        
        chackThatFunAndProcAreInstalledOnlyInTheSpecifiedDb(testSession, 
                "CALL custom.testProc", "RETURN custom.testFun() AS answer",
                fooSession, neo4jSession);
    }

    @Test
    public void testProceduresFunctionsWithSameNameGlobalAndSpecificDbCoexist() {
        String procName = "sameName.foo.proc";
        String funName = "sameName.foo.fun";

        installProcsAndFuncsAndCheckExist(procName, funName);

        // installing the same name/signature into a specific db must now coexist with the global ("_all")
        // registration, with the specific db's version taking precedence there
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installProcedure('" + procName + "() :: (answer::INT)','RETURN 99 as answer', $db)",
                        Map.of("db", DB_TEST)).consume()
        );
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installFunction('" + funName + "() :: INT','RETURN 99 as answer', $db)",
                        Map.of("db", DB_TEST)).consume()
        );
        testSession.executeWrite(tx -> tx.run("call db.clearQueryCaches()").consume());

        checkInstalled(testSession, "CALL custom." + procName, 99L);
        checkInstalled(testSession, "RETURN custom." + funName + "() AS answer", 99L);

        // other databases are still served by the global version
        checkInstalled(fooSession, "CALL custom." + procName, 42L);
        checkInstalled(neo4jSession, "RETURN custom." + funName + "() AS answer", 42L);

        systemSession.executeWrite(
                tx -> tx.run("CALL apoc.custom.dropAll(null)").consume()
        );
        systemSession.executeWrite(tx -> {
            tx.run("CALL apoc.custom.dropProcedure('" + procName + "', $db)", Map.of("db", DB_TEST)).consume();
            tx.run("CALL apoc.custom.dropFunction('" + funName + "', $db)", Map.of("db", DB_TEST)).consume();
            return null;
        });

        checkProcsAndFuncsAreDeleted();
    }

    @Test
    public void testProceduresFunctionsWithSameNameInTwoDbsCoexist() {
        String procName = "sameNameFooProc";
        String funName = "sameNameFooFun";

        // install a procedure and a function for foo database
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installProcedure('" + procName + "() :: (answer::INT)','RETURN 42 as answer', $db)",
                        Map.of("db", DB_FOO)).consume()
        );
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installFunction('" + funName + "() :: INT','RETURN 42 as answer', $db)",
                        Map.of("db", DB_FOO)).consume()
        );

        checkInstalled(fooSession, "CALL custom." + procName);
        checkInstalled(fooSession, "RETURN custom." + funName + "() AS answer");

        // installing the same name/signature into a second, different db must now succeed and coexist,
        // instead of failing - this is the fix for #4570
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installProcedure('" + procName + "() :: (answer::INT)','RETURN 7 as answer', $db)",
                        Map.of("db", DB_TEST)).consume()
        );
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installFunction('" + funName + "() :: INT','RETURN 7 as answer', $db)",
                        Map.of("db", DB_TEST)).consume()
        );

        checkInstalled(fooSession, "CALL custom." + procName, 42L);
        checkInstalled(fooSession, "RETURN custom." + funName + "() AS answer", 42L);
        checkInstalled(testSession, "CALL custom." + procName, 7L);
        checkInstalled(testSession, "RETURN custom." + funName + "() AS answer", 7L);

        // dropping in one db must not affect the other
        systemSession.executeWrite(tx -> {
            tx.run("CALL apoc.custom.dropProcedure('" + procName + "', $db)", Map.of("db", DB_FOO)).consume();
            tx.run("CALL apoc.custom.dropFunction('" + funName + "', $db)", Map.of("db", DB_FOO)).consume();
            return null;
        });

        checkCustomProcAndFunNotInstalled("CALL custom." + procName, "RETURN custom." + funName + "() AS answer", fooSession);
        checkInstalled(testSession, "CALL custom." + procName, 7L);
        checkInstalled(testSession, "RETURN custom." + funName + "() AS answer", 7L);

        systemSession.executeWrite(tx -> {
            tx.run("CALL apoc.custom.dropProcedure('" + procName + "', $db)", Map.of("db", DB_TEST)).consume();
            tx.run("CALL apoc.custom.dropFunction('" + funName + "', $db)", Map.of("db", DB_TEST)).consume();
            return null;
        });

        checkProcsAndFuncsAreDeleted();
    }

    @Test
    public void testProceduresFunctionsWithIncompatibleSignatureInAnotherDbFails() {
        String procName = "incompatibleFooProc";
        String funName = "incompatibleFooFun";

        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installProcedure('" + procName + "() :: (answer::INT)','RETURN 42 as answer', $db)",
                        Map.of("db", DB_FOO)).consume()
        );
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installFunction('" + funName + "() :: INT','RETURN 42 as answer', $db)",
                        Map.of("db", DB_FOO)).consume()
        );

        try {
            systemSession.executeWrite(tx ->
                    tx.run("CALL apoc.custom.installProcedure('" + procName + "(xx :: STRING) :: (answer::INT)','RETURN 42 as answer', $db)",
                            Map.of("db", DB_TEST)).consume()
            );
            fail("Should fail due to incompatible signature already registered in another db");
        } catch (Exception e) {
            assertTrue("Actual err. message is: " + e.getMessage(), e.getMessage().contains("incompatible signature"));
        }

        try {
            systemSession.executeWrite(tx ->
                    tx.run("CALL apoc.custom.installFunction('" + funName + "() :: STRING','RETURN \"42\" as answer', $db)",
                            Map.of("db", DB_TEST)).consume()
            );
            fail("Should fail due to incompatible signature already registered in another db");
        } catch (Exception e) {
            assertTrue("Actual err. message is: " + e.getMessage(), e.getMessage().contains("incompatible signature"));
        }

        systemSession.executeWrite(tx -> {
            tx.run("CALL apoc.custom.dropProcedure('" + procName + "', $db)", Map.of("db", DB_FOO)).consume();
            tx.run("CALL apoc.custom.dropFunction('" + funName + "', $db)", Map.of("db", DB_FOO)).consume();
            return null;
        });
    }

    @Test
    public void testProceduresFunctionsInstalledGlobally() {

        String procName = "barProc";
        String funName = "barFun";

        installAndDropProcsAndFuncsGlobally(procName, funName);
    }

    private static void installAndDropProcsAndFuncsGlobally(String procName, String funName)  {
        // -- using dropProcedure and dropFunction
        installProcsAndFuncsAndCheckExist(procName, funName);

        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.dropProcedure('" + procName + "',null)",
                        Map.of("db", DB_FOO)).consume()
        );
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.dropFunction('" + funName + "', null)",
                        Map.of("db", DB_FOO)).consume()
        );
        
        checkProcsAndFuncsAreDeleted();

        // -- using dropAll
        installProcsAndFuncsAndCheckExist(procName, funName);

        systemSession.executeWrite(
                tx -> tx.run("CALL apoc.custom.dropAll(null)").consume()
        );

        checkProcsAndFuncsAreDeleted();
    }

    private static void checkProcsAndFuncsAreDeleted() {
        String countCustomGlobals = "CALL apoc.custom.show(null) YIELD name RETURN count(*) AS count";
        assertEventually(() -> (long) singleResultFirstColumn(systemSession, countCustomGlobals),
                (value) -> value == 0L, TIMEOUT, SECONDS);

        for (String db: ALL_DB_NAMES) {
            String countCustomLocal = "CALL apoc.custom.show('" + db + "') YIELD name RETURN count(*) AS count";
            long count = singleResultFirstColumn(systemSession, countCustomLocal);
            assertEquals(0, count);
        }
        
        String countCustom = "CALL apoc.custom.list() YIELD name RETURN count(*) AS count";
        assertEventually(() -> (long) singleResultFirstColumn(testSession, countCustom),
                (value) -> value == 0L, TIMEOUT, SECONDS);
    }

    private static void installProcsAndFuncsAndCheckExist(String procName, String funName) {
        // install a procedure and a function for foo database
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installProcedure('" + procName + "() :: (answer::INT)','RETURN 42 as answer', null)",
                        Map.of("db", DB_FOO)).consume()
        );
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installFunction('" + funName + "() :: INT','RETURN 42 as answer', null)",
                        Map.of("db", DB_FOO)).consume()
        );

        checkInstalled(testSession, "CALL custom." + procName);
        checkInstalled(testSession, "RETURN custom." + funName + "() AS answer");

        checkInstalled(fooSession, "CALL custom." + procName);
        checkInstalled(fooSession, "RETURN custom." + funName + "() AS answer");

        checkInstalled(neo4jSession, "CALL custom." + procName);
        checkInstalled(neo4jSession, "RETURN custom." + funName + "() AS answer");

        // check that the procedure are global, not local
        String countCustomGlobals = "CALL apoc.custom.show(null) YIELD name RETURN count(*) AS count";
        assertEventually(() -> (long) singleResultFirstColumn(systemSession, countCustomGlobals),
                (value) -> value == 2L, TIMEOUT, SECONDS);

        for (String db: ALL_DB_NAMES) {
            String countCustomLocal = "CALL apoc.custom.show('" + db + "') YIELD name RETURN count(*) AS count";
            long count = singleResultFirstColumn(systemSession, countCustomLocal);
            assertEquals(0, count);
        }
    }


    @Test
    public void testProceduresFunctionsInDatabaseAlias() {
        systemSession.executeWrite(tx -> tx.run("CREATE ALIAS `test-alias` FOR DATABASE dbfoo",
                        Map.of("db", DB_TEST)).consume()
        );

        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installProcedure('testAliasProc() :: (answer::INT)','RETURN 42 as answer', 'test-alias')")
                        .consume()
        );
        
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installFunction('testAliasFun() :: INT','RETURN 42 as answer', 'test-alias')")
                        .consume()
        );

        chackThatFunAndProcAreInstalledOnlyInTheSpecifiedDb(fooSession,
                "CALL custom.testAliasProc", 
                "RETURN custom.testAliasFun() AS answer",
                neo4jSession, testSession);

        systemSession.executeWrite(tx -> {
                tx.run("CALL apoc.custom.dropProcedure('testAliasProc', 'test-alias')")
                        .consume();
                tx.run("CALL apoc.custom.dropFunction('testAliasFun', 'test-alias')")
                        .consume();
                return null;
        });

        String countCustom = "CALL apoc.custom.show('test-alias') YIELD name RETURN count(*) AS count";
        long count = singleResultFirstColumn(systemSession, countCustom);
        assertEquals(0, count);
        
    }

    private static void installTestProcAndFun() {
        systemSession.executeWrite(tx -> 
                tx.run("CALL apoc.custom.installProcedure('testProc() :: (answer::INT)','RETURN 42 as answer', $db)",
                        Map.of("db", DB_TEST)).consume()
        );
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installFunction('testFun() :: INT','RETURN 42 as answer', $db)",
                        Map.of("db", DB_TEST)).consume()
        );
    }

    private static void installFooProcAndFun() {
        systemSession.executeWrite(tx -> 
                tx.run("CALL apoc.custom.installProcedure('fooProc() :: (answer::INT)','RETURN 42 as answer', $db)",
                        Map.of("db", DB_FOO)).consume()
        );
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installFunction('fooFun() :: INT','RETURN 42 as answer', $db)",
                        Map.of("db", DB_FOO)).consume()
        );
    }

    private static void installNeo4jProcAndFun() {
        systemSession.executeWrite(tx -> 
                tx.run("CALL apoc.custom.installProcedure('neo4jProc() :: (answer::INT)','RETURN 42 as answer', $db)", 
                        Map.of("db", DEFAULT_DATABASE_NAME)).consume()
        );
        systemSession.executeWrite(tx ->
                tx.run("CALL apoc.custom.installFunction('neo4jFun() :: INT','RETURN 42 as answer', $db)",
                        Map.of("db", DEFAULT_DATABASE_NAME)).consume()
        );
    }

    private static void chackThatFunAndProcAreInstalledOnlyInTheSpecifiedDb(Session sessionWithCustomInstalled, String neo4jProcedure, String neo4jFunction, Session otherSessionDb, Session otherSessionDb2) {
        checkInstalled(sessionWithCustomInstalled, neo4jProcedure);
        checkInstalled(sessionWithCustomInstalled, neo4jFunction);

        checkCustomProcAndFunNotInstalled(neo4jProcedure, neo4jFunction, otherSessionDb);

        checkCustomProcAndFunNotInstalled(neo4jProcedure, neo4jFunction, otherSessionDb2);
    }

    private static void checkInstalled(Session sessionWithCustom, String neo4jProc) {
        checkInstalled(sessionWithCustom, neo4jProc, 42L);
    }

    private static void checkInstalled(Session sessionWithCustom, String neo4jProc, long expected) {
        assertEventually(() -> {
                    try {
                        long res = singleResultFirstColumn(sessionWithCustom, neo4jProc);
                        return expected == res;
                    } catch (Exception e) {
                        return false;
                    }
                },
                (v) -> v, TIMEOUT, SECONDS);
    }

    private static void checkCustomProcAndFunNotInstalled(String neo4jProc, String neo4jFun, Session otherSessionDb) {
        try {
            testCall(otherSessionDb, neo4jProc, r -> fail("Should fail due to not existing procedure "));
        } catch (Exception e) {
            String actualErrMsg = e.getMessage();
            assertTrue("Actual err. message is: " + actualErrMsg, actualErrMsg.contains("There is no procedure with the name"));
        }

        try {
            testCall(otherSessionDb, neo4jFun, r -> fail("Should fail due to not existing function"));
        } catch (Exception e) {
            String actualErrMsg = e.getMessage();
            assertTrue("Actual err. message is: " + actualErrMsg, actualErrMsg.contains("Unknown function"));
        }
    }

    private static void createDatabases() {
        try(Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            systemSession.executeWrite(tx -> {
                tx.run("CREATE DATABASE " + DB_TEST + " WAIT;").consume();
                tx.run("CREATE DATABASE " + DB_FOO + " WAIT;").consume();
                return null;
            });
        }

        try(Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            assertEventually(() -> {
                final List<Record> list = systemSession.run("SHOW DATABASES YIELD name, currentStatus")
                        .list();
                return list.stream().allMatch(i -> i.get("currentStatus").asString().equals("online"))
                        && list.stream().map(i -> i.get("name").asString()).toList().containsAll(List.of(DB_TEST , DB_FOO));
            }, value -> value, 30L, TimeUnit.SECONDS);
        }
    }

    private static void createSessions() {
        neo4jSession = neo4jContainer.getSession();
        systemSession = driver.session(SessionConfig.forDatabase(SYSTEM_DATABASE_NAME));
        testSession = driver.session(SessionConfig.forDatabase(DB_TEST));
        fooSession = driver.session(SessionConfig.forDatabase(DB_FOO));
    }

}
