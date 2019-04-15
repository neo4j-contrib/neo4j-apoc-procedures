package apoc.custom;


import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.TestcontainersCausalCluster;
import apoc.util.Util;
import org.junit.*;

import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;


public class CypherProceduresClusterTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        executeGradleTasks("clean", "shadow");
        TestUtil.ignoreException(() ->  cluster = TestContainerUtil
                .createEnterpriseCluster(3, 1, Util.map("apoc.custom.procedures.refresh", 100)),
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

    @Before
    public void clearCache() {
        cluster.getSession().writeTransaction(tx -> tx.run("call dbms.clearQueryCaches()")); // we update the function
    }

    @Test
    public void shouldRecreateCustomFunctionsOnOtherClusterMembers() {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer1', 'RETURN 42 as answer')")); // we create a function

        // when
        testCall(cluster.getSession(), "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        testCallInReadTransaction(cluster.getSession(), "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
    }

    @Test
    public void shouldUpdateCustomFunctionsOnOtherClusterMembers() {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer2', 'RETURN 42 as answer')")); // we create a function
        testCall(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer2', 'RETURN 52 as answer')")); // we update the function
        clearCache();

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        testCallInReadTransaction(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(52L, ((Map)((List)row.get("row")).get(0)).get("answer")));
    }

    @Test
    public void shouldRegisterSimpleStatementOnOtherClusterMembers() {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure1', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure

        // when
        testCall(cluster.getSession(), "call custom.answerProcedure1()", (row) -> assertEquals(33L, row.get("answer")));

        // then
        testCallInReadTransaction(cluster.getSession(), "call custom.answerProcedure1()", (row) -> assertEquals(33L, row.get("answer")));
    }

    @Test
    public void shouldUpdateSimpleStatementOnOtherClusterMembers() {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure2', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure
        testCall(cluster.getSession(), "call custom.answerProcedure2()", (row) -> assertEquals(33L, row.get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure2', 'RETURN 55 as answer', 'read', [['answer','long']])")); // we create a procedure
        clearCache();

        // then
        testCallInReadTransaction(cluster.getSession(), "call custom.answerProcedure2()", (row) -> assertEquals(55L, row.get("answer")));
    }
}
