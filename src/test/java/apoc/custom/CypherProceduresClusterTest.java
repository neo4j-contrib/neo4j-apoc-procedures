package apoc.custom;


import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.TestcontainersCausalCluster;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static apoc.util.TestContainerUtil.cleanBuild;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallInReadTransaction;
import static apoc.util.TestUtil.isTravis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;


public class CypherProceduresClusterTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() ->  cluster = TestContainerUtil
                        .createEnterpriseCluster(3, 1, Collections.emptyMap(), MapUtil.stringMap("apoc.custom.procedures.refresh", "100")),
                Exception.class);
        assumeNotNull(cluster);
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
        cleanBuild();
    }

    @Test
    public void shouldRecreateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer1', 'RETURN 42 as answer')")); // we create a function

        // when
        testCall(cluster.getSession(), "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        Thread.sleep(1000);

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        testCallInReadTransaction(cluster.getSession(), "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
    }

    @Test
    public void shouldUpdateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer2', 'RETURN 42 as answer')")); // we create a function
        testCall(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer2', 'RETURN 52 as answer')")); // we update the function
        Thread.sleep(1000);

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        testCallInReadTransaction(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(52L, ((Map)((List)row.get("row")).get(0)).get("answer")));
    }

    @Test
    public void shouldRegisterSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure1', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure

        // when
        testCall(cluster.getSession(), "call custom.answerProcedure1()", (row) -> assertEquals(33L, row.get("answer")));
        Thread.sleep(1000);
        // then
        testCallInReadTransaction(cluster.getSession(), "call custom.answerProcedure1()", (row) -> assertEquals(33L, row.get("answer")));
    }

    @Test
    public void shouldUpdateSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure2', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure
        testCall(cluster.getSession(), "call custom.answerProcedure2()", (row) -> assertEquals(33L, row.get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure2', 'RETURN 55 as answer', 'read', [['answer','long']])")); // we create a procedure

        Thread.sleep(1000);
        // then
        testCallInReadTransaction(cluster.getSession(), "call custom.answerProcedure2()", (row) -> assertEquals(55L, row.get("answer")));
    }

    @Test(expected = DatabaseException.class)
    public void shouldRemoveProcedureOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerToRemove', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure
        Thread.sleep(1000);
        try {
            testCallInReadTransaction(cluster.getSession(), "call custom.answerToRemove()", (row) -> assertEquals(33L, row.get("answer")));
        } catch (Exception e) {
            fail("Exception while calling the procedure");
        }

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.removeProcedure('answerToRemove')")); // we remove procedure

        // then
        Thread.sleep(1000);
        try {
            testCallInReadTransaction(cluster.getSession(), "call custom.answerToRemove()", (row) -> fail("Procedure not removed"));
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
            testCallInReadTransaction(cluster.getSession(), "return custom.answerFunctionToRemove() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        } catch (Exception e) {
            fail("Exception while calling the function");
        }

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.removeFunction('answerFunctionToRemove')")); // we remove procedure

        // then
        Thread.sleep(1000);
        try {
            testCallInReadTransaction(cluster.getSession(), "return custom.answerFunctionToRemove() as row", (row) -> fail("Function not removed"));
        } catch (DatabaseException e) {
            String expectedMessage = "Unknown function 'custom.answerFunctionToRemove'";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void overrideCallStatementWithNewArguments() throws Exception {
        // given
        // adding 2 procedures
        cluster.getSession().run("call apoc.custom.asProcedure('answer','RETURN 42 as answer', 'read', [['answer', 'number']])");
        cluster.getSession().run("call apoc.custom.asProcedure('answer2','RETURN 32 as answer', 'read', [['answer', 'number']])");
        testCallInReadTransaction(cluster.getSession(), "call custom.answer() yield answer return answer", (row) -> assertEquals(42L, row.get("answer")));
        testCallInReadTransaction(cluster.getSession(), "call custom.answer2() yield answer return answer", (row) -> assertEquals(32L, row.get("answer")));

        // when
        // removing one procedure
        cluster.getSession().run("call apoc.custom.removeProcedure('answer')");
        try {
            org.neo4j.test.assertion.Assert.<Boolean, Exception>assertEventually(() ->
                    cluster.getSession().readTransaction((tx) -> {
                        List<Map<String, Object>> list = tx.run("call apoc.custom.list()")
                                .stream()
                                .map(Record::asMap)
                                .collect(Collectors.toList());
                        return list.stream().anyMatch(map -> "answer2".equals(map.get("name")));
                    }), Matchers.equalTo(true), 1000, TimeUnit.MILLISECONDS);
            cluster.getSession().run("call custom.answer()").consume();
            Assert.fail("procedure not removed");
        } catch (Exception e) {
            assertTrue(e instanceof DatabaseException);
        }
        // creating a new one, this triggers the scheduler
        cluster.getSession().run("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']], 'Procedure that answer to the Ultimate Question of Life, the Universe, and Everything')");
        org.neo4j.test.assertion.Assert.<Boolean, Exception>assertEventually(() ->
                cluster.getSession().readTransaction((tx) -> {
                    List<Map<String, Object>> list = tx.run("call apoc.custom.list()")
                            .stream()
                            .map(Record::asMap)
                            .collect(Collectors.toList());
                    return list.stream().anyMatch(map -> "answer".equals(map.get("name")) && "procedure".equals(map.get("type")))
                            && list.stream().anyMatch(map -> "answer2".equals(map.get("name")));
                }), Matchers.equalTo(true), 1000, TimeUnit.MILLISECONDS);

        // then
        // testing the new procedure
        testCallInReadTransaction(cluster.getSession(), "call custom.answer(1)", (row) -> assertEquals(1L, row.get("answer")));
        // we add a new function to trigger again the scheduler
        cluster.getSession().run("call apoc.custom.asFunction('answer','RETURN 42','long')");
        // waiting that the scheduler has been triggered
        org.neo4j.test.assertion.Assert.<Boolean, Exception>assertEventually(() ->
            cluster.getSession().readTransaction((tx) -> {
                List<Map<String, Object>> list = tx.run("call apoc.custom.list()")
                    .stream()
                    .map(Record::asMap)
                    .collect(Collectors.toList());
                return list.stream().anyMatch(map -> "answer".equals(map.get("name")) && "function".equals(map.get("type")))
                    && list.stream().anyMatch(map -> "answer".equals(map.get("name")) && "procedure".equals(map.get("type")))
                    && list.stream().anyMatch(map -> "answer2".equals(map.get("name")));
            }), Matchers.equalTo(true), 1000, TimeUnit.MILLISECONDS);


        // testing again the procedure
        testCallInReadTransaction(cluster.getSession(), "call custom.answer(1)", (row) -> assertEquals(1L, row.get("answer")));
    }
}
