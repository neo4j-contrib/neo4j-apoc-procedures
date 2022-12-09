package apoc.custom;

import apoc.util.ExtendedTestContainerUtil;
import apoc.util.TestContainerUtil;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.TestcontainersCausalCluster;
import org.junit.*;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.DatabaseException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


// TODO: Remove the @Ignore annotations after fixing clustering
// I investigated the clustering setup over the course of a couple of days and could not get it to work either. Nor our
// existing setup or nor Michael's setup [1] work reliably 100% of the time. There is a real possibility
// that clustering might be broken in dev because it is undergoing many changes.
// [1] https://github.com/michael-simons/junit-jupiter-causal-cluster-testcontainer-extension
public class CypherProceduresClusterTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        cluster = ExtendedTestContainerUtil.createEnterpriseCluster(
                List.of(ApocPackage.EXTENDED),
                3,
                1,
                Collections.emptyMap(),
                Map.of("apoc.custom.procedures.refresh", "100"));
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

    @Test
    @Ignore
    public void shouldRecreateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        
        try(Session session = cluster.getDriver().session()) {
            session.writeTransaction(tx -> tx.run("call apoc.custom.declareFunction('answer1() :: (output::LONG)', 'RETURN 42 as answer')")); // we create a function
        }

        // whencypher procedures
        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCall(session, "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        }

        Thread.sleep(1000);

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        try(Session session = cluster.getDriver().session()) {
            ExtendedTestContainerUtil.testCallInReadTransaction(session, "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        }
    }

    @Test
    @Ignore
    public void shouldUpdateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.declareFunction('answer2() :: (output::LONG)', 'RETURN 42 as answer')")); // we create a function
        TestContainerUtil.testCall(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.declareFunction('answer2() :: (output::LONG)', 'RETURN 52 as answer')")); // we update the function
        Thread.sleep(1000);

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        ExtendedTestContainerUtil.testCallInReadTransaction(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(52L, ((Map)((List)row.get("row")).get(0)).get("answer")));
    }

    @Test
    @Ignore
    public void shouldRegisterSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.declareProcedure('answerProcedure1() :: LONG', 'RETURN 33 as answer', 'read'")); // we create a procedure

        // when
        TestContainerUtil.testCall(cluster.getSession(), "call custom.answerProcedure1()", (row) -> Assert.assertEquals(33L, row.get("answer")));
        Thread.sleep(1000);
        // then
        ExtendedTestContainerUtil.testCallInReadTransaction(cluster.getSession(), "call custom.answerProcedure1()", (row) -> Assert.assertEquals(33L, row.get("answer")));
    }

    @Test
    @Ignore
    public void shouldUpdateSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.declareProcedure('answerProcedure2() :: LONG', 'RETURN 33 as answer')")); // we create a procedure
        TestContainerUtil.testCall(cluster.getSession(), "call custom.answerProcedure2()", (row) -> Assert.assertEquals(33L, row.get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.declareProcedure('answerProcedure2() :: LONG', 'RETURN 55 as answer')")); // we create a procedure

        Thread.sleep(1000);
        // then
        ExtendedTestContainerUtil.testCallInReadTransaction(cluster.getSession(), "call custom.answerProcedure2()", (row) -> Assert.assertEquals(55L, row.get("answer")));
    }

    @Test(expected = DatabaseException.class)
    @Ignore
    public void shouldRemoveProcedureOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.declareProcedure('answerToRemove() :: LONG', 'RETURN 33 as answer')")); // we create a procedure
        Thread.sleep(1000);
        try {
            ExtendedTestContainerUtil.testCallInReadTransaction(cluster.getSession(), "call custom.answerToRemove()", (row) -> Assert.assertEquals(33L, row.get("answer")));
        } catch (Exception e) {
            fail("Exception while calling the procedure");
        }

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.removeProcedure('answerToRemove')")); // we remove procedure

        // then
        Thread.sleep(1000);
        System.out.println("waited 5000ms");
        try {
            ExtendedTestContainerUtil.testCallInReadTransaction(cluster.getSession(), "call custom.answerToRemove()", (row) -> fail("Procedure not removed"));
        } catch (DatabaseException e) {
            String expectedMessage = "There is no procedure with the name `custom.answerToRemove` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test(expected = DatabaseException.class)
    @Ignore
    public void shouldRemoveFunctionOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.declareFunction('answerFunctionToRemove()', 'RETURN 42 as answer')")); // we create a function
        Thread.sleep(1000);
        try {
            ExtendedTestContainerUtil.testCallInReadTransaction(cluster.getSession(), "return custom.answerFunctionToRemove() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        } catch (Exception e) {
            fail("Exception while calling the function");
        }

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.removeFunction('answerFunctionToRemove')")); // we remove procedure

        // then
        Thread.sleep(1000);
        try {
            ExtendedTestContainerUtil.testCallInReadTransaction(cluster.getSession(), "return custom.answerFunctionToRemove() as row", (row) -> fail("Function not removed"));
        } catch (DatabaseException e) {
            String expectedMessage = "Unknown function 'custom.answerFunctionToRemove'";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }
}
