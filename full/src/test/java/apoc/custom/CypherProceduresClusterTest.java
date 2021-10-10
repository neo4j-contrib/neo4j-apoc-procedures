package apoc.custom;

import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class CypherProceduresClusterTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() ->  cluster = TestContainerUtil
                .createEnterpriseCluster(3, 1, Collections.emptyMap(), MapUtil.stringMap("apoc.custom.procedures.refresh", "100")),
                Exception.class);
        Assume.assumeNotNull(cluster);
        assumeTrue("Neo4j Cluster should be up-and-running", cluster.isRunning());
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void shouldRecreateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        
        try(Session session = cluster.getDriver().session()) {
            session.writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer1', 'RETURN 42 as answer')")); // we create a function
        }

        // whencypher procedures
        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCall(session, "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        }

        Thread.sleep(1000);

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCallInReadTransaction(session, "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        }
    }

    @Test
    public void shouldUpdateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer2', 'RETURN 42 as answer')")); // we create a function
        TestContainerUtil.testCall(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer2', 'RETURN 52 as answer')")); // we update the function
        Thread.sleep(1000);

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        TestContainerUtil.testCallInReadTransaction(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(52L, ((Map)((List)row.get("row")).get(0)).get("answer")));
    }

    @Test
    public void shouldRegisterSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure1', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure

        // when
        TestContainerUtil.testCall(cluster.getSession(), "call custom.answerProcedure1()", (row) -> Assert.assertEquals(33L, row.get("answer")));
        Thread.sleep(1000);
        // then
        TestContainerUtil.testCallInReadTransaction(cluster.getSession(), "call custom.answerProcedure1()", (row) -> Assert.assertEquals(33L, row.get("answer")));
    }

    @Test
    public void shouldUpdateSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure2', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure
        TestContainerUtil.testCall(cluster.getSession(), "call custom.answerProcedure2()", (row) -> Assert.assertEquals(33L, row.get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure2', 'RETURN 55 as answer', 'read', [['answer','long']])")); // we create a procedure

        Thread.sleep(1000);
        // then
        TestContainerUtil.testCallInReadTransaction(cluster.getSession(), "call custom.answerProcedure2()", (row) -> Assert.assertEquals(55L, row.get("answer")));
    }

    @Test(expected = DatabaseException.class)
    public void shouldRemoveProcedureOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerToRemove', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure
        Thread.sleep(1000);
        try {
            TestContainerUtil.testCallInReadTransaction(cluster.getSession(), "call custom.answerToRemove()", (row) -> Assert.assertEquals(33L, row.get("answer")));
        } catch (Exception e) {
            fail("Exception while calling the procedure");
        }

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.removeProcedure('answerToRemove')")); // we remove procedure

        // then
        Thread.sleep(1000);
        System.out.println("waited 5000ms");
        try {
            TestContainerUtil.testCallInReadTransaction(cluster.getSession(), "call custom.answerToRemove()", (row) -> fail("Procedure not removed"));
        } catch (DatabaseException e) {
            String expectedMessage = "There is no procedure with the name `custom.answerToRemove` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test(expected = DatabaseException.class)
    public void shouldRemoveFunctionOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answerFunctionToRemove', 'RETURN 42 as answer')")); // we create a function
        Thread.sleep(1000);
        try {
            TestContainerUtil.testCallInReadTransaction(cluster.getSession(), "return custom.answerFunctionToRemove() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        } catch (Exception e) {
            fail("Exception while calling the function");
        }

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.removeFunction('answerFunctionToRemove')")); // we remove procedure

        // then
        Thread.sleep(1000);
        try {
            TestContainerUtil.testCallInReadTransaction(cluster.getSession(), "return custom.answerFunctionToRemove() as row", (row) -> fail("Function not removed"));
        } catch (DatabaseException e) {
            String expectedMessage = "Unknown function 'custom.answerFunctionToRemove'";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }
}
