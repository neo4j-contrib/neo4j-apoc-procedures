package apoc.custom;

import apoc.util.ExtendedTestContainerUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.*;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.DatabaseException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static apoc.util.ExtendedTestContainerUtil.dbIsWriter;
import static apoc.util.ExtendedTestContainerUtil.getBoltAddress;
import static apoc.util.ExtendedTestContainerUtil.getSessionForDb;
import static apoc.util.ExtendedTestContainerUtil.testCallEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.internal.DatabaseNameUtil.SYSTEM_DATABASE_NAME;

public class CypherProceduresClusterTest {

    private static TestcontainersCausalCluster cluster;
    private static Session writeSession;
    private static Session readSession;

    @BeforeClass
    public static void setupCluster() {
        cluster = ExtendedTestContainerUtil.createEnterpriseCluster(
                List.of(TestContainerUtil.ApocPackage.EXTENDED),
                3,
                0,
                Collections.emptyMap(),
                Map.of("NEO4J_dbms_routing_enabled", "true",
                        "apoc.custom.procedures.refresh", "100"));

        getSystemLeaderAndFollower();
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

    @Test
    public void shouldRecreateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.declareFunction('answer1() :: LONG', 'RETURN 42 as answer')")); // we create a function

        // whencypher procedures
        TestContainerUtil.testCall(writeSession, "return custom.answer1() as row", (row) -> assertEquals(42L, row.get("row")));

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        ExtendedTestContainerUtil.testCallEventuallyInReadTransaction(readSession, "return custom.answer1() as row", (row) -> assertEquals(42L, row.get("row")), 10L);
    }

    @Test
    public void shouldUpdateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.declareFunction('answer2() :: LONG', 'RETURN 42 as answer')")); // we create a function
        TestContainerUtil.testCall(writeSession, "return custom.answer2() as row", (row) -> assertEquals(42L, row.get("row")));

        // when
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.declareFunction('answer2() :: LONG', 'RETURN 52 as answer')")); // we update the function

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        ExtendedTestContainerUtil.testCallInReadTransaction(readSession, "return custom.answer2() as row", (row) -> assertEquals(52L, row.get("row")));
    }

    @Test
    public void shouldRegisterSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.declareProcedure('answerProcedure1() :: (answer :: LONG)', 'RETURN 33 as answer', 'read')")); // we create a procedure

        // when
        TestContainerUtil.testCall(writeSession, "call custom.answerProcedure1()", (row) -> Assert.assertEquals(33L, row.get("answer")));

        // then
        ExtendedTestContainerUtil.testCallEventuallyInReadTransaction(readSession, "call custom.answerProcedure1()", (row) -> Assert.assertEquals(33L, row.get("answer")), 10L);
    }

    @Test
    public void shouldUpdateSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.declareProcedure('answerProcedure2() :: (answer::LONG)', 'RETURN 33 as answer')")); // we create a procedure
        TestContainerUtil.testCall(writeSession, "call custom.answerProcedure2()", (row) -> Assert.assertEquals(33L, row.get("answer")));

        // when
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.declareProcedure('answerProcedure2() :: (answer::LONG)', 'RETURN 55 as answer')")); // we create a procedure

        // then
        ExtendedTestContainerUtil.testCallEventuallyInReadTransaction(readSession, "call custom.answerProcedure2()", (row) -> Assert.assertEquals(55L, row.get("answer")), 10L);
    }

    @Test(expected = DatabaseException.class)
    public void shouldRemoveProcedureOnOtherClusterMembers() throws InterruptedException {
        // given
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.declareProcedure('answerToRemove() :: (answer::LONG)', 'RETURN 33 as answer')")); // we create a procedure
        ExtendedTestContainerUtil.testCallEventuallyInReadTransaction(readSession, "call custom.answerToRemove()", (row) -> Assert.assertEquals(33L, row.get("answer")), 10L);

        // when
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.removeProcedure('answerToRemove')")); // we remove procedure

        Thread.sleep(1000);
        // then
        try {
            TestContainerUtil.testCall(readSession, "call custom.answerToRemove()", (row) -> fail("Procedure not removed"));
        } catch (DatabaseException e) {
            String expectedMessage = "There is no procedure with the name `custom.answerToRemove` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test(expected = DatabaseException.class)
    public void shouldRemoveFunctionOnOtherClusterMembers() throws InterruptedException {
        // given
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.declareFunction('answerFunctionToRemove() :: LONG', 'RETURN 42 as answer')")); // we create a function
        ExtendedTestContainerUtil.testCallEventuallyInReadTransaction(readSession, "return custom.answerFunctionToRemove() as row", (row) -> assertEquals(42L, row.get("row")), 10L);

        // when
        writeSession.writeTransaction(tx -> tx.run("call apoc.custom.removeFunction('answerFunctionToRemove')")); // we remove procedure

        Thread.sleep(1000);
        // then
        try {
            TestContainerUtil.testCall(readSession, "return custom.answerFunctionToRemove() as row", (row) -> fail("Function not removed"));
        } catch (DatabaseException e) {
            String expectedMessage = "Unknown function 'custom.answerFunctionToRemove'";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testRestoreProcedureWorksCorrectlyOnOtherClusterMembers() {
        List<String> listProcNames = IntStream.range(0, 10)
                .mapToObj(i -> "proc" + i)
                .toList();

        // for each element, declare a procedure with that name,
        // then call the custom procedure
        // and finally overwrite and re-call it
        listProcNames.forEach(name -> {
            String declareProcedure = String.format("CALL apoc.custom.declareProcedure('%s() :: (answer::INT)', $query)", name);
            String callProcedure = String.format("call custom.%s", name);

            writeSession.writeTransaction(
                    tx -> tx.run(declareProcedure, Map.of("query", "RETURN 42 AS answer"))
            );

            // test that it's work for each node
            cluster.getClusterMembers().forEach(container -> {
                testCallEventually(container.getSession(), callProcedure, Map.of(),
                        row -> assertEquals(42L, row.get("answer")),
                        20L);
            });

            // overwriting on the leader
            writeSession.writeTransaction(
                    tx -> tx.run(declareProcedure, Map.of("query", "RETURN 1 AS answer"))
            );

            // check that it has been updated for each node
            cluster.getClusterMembers().forEach(container -> {
                testCallEventually(container.getSession(), callProcedure, Map.of(),
                        row -> assertEquals(1L, row.get("answer")),
                        20L);
            });
        });
    }

    @Test
    public void testRestoreFunctionWorksCorrectlyOnOtherClusterMembers() {
        // create a list of ["fun1", "fun2", "fun3" ....] strings
        List<String> listFunNames = IntStream.range(0, 10)
                .mapToObj(i -> "fun" + i)
                .toList();

        // for each element, declare a function with that name,
        // then call the custom function
        // and finally overwrite and re-call it
        listFunNames.forEach(name -> {
            final String declareFunction = String.format("CALL apoc.custom.declareFunction('%s() :: INT', $query)", name);
            final String funQuery = String.format("return custom.%s() as row", name);

            writeSession.writeTransaction(tx -> tx.run(declareFunction,
                    Map.of("query", "RETURN 42 as answer")
            ));

            // test that it's work each node
            cluster.getClusterMembers().forEach(container -> {
                testCallEventually(container.getSession(), funQuery, Map.of(),
                        row -> assertEquals(42L, row.get("row")),
                        20L);
            });

            // overwriting on the leader
            writeSession.writeTransaction(
                    tx -> tx.run(declareFunction, Map.of("query", "RETURN 1 AS answer"))
            );

            // check that it has been updated for each node
            cluster.getClusterMembers().forEach(container -> {
                testCallEventually(container.getSession(), funQuery, Map.of(),
                        row -> assertEquals(1L, row.get("row")),
                        20L);
            });
        });
    }

    private static void getSystemLeaderAndFollower() {
        for (Neo4jContainerExtension instance: cluster.getClusterMembers()) {
            Session session = getSessionForDb(instance, DEFAULT_DATABASE_NAME);

            if (dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(instance))) {
                writeSession = session;
            } else {
                readSession = session;
            }
        }
        if (writeSession == null) {
            throw new RuntimeException("No system db leader found");
        }
        if (readSession == null) {
            throw new RuntimeException("No system db follower found");
        }
    }
}
