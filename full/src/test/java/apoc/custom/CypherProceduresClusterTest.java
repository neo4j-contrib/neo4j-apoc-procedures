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

import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.*;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static apoc.util.TestContainerUtil.testCallEventually;
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

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCallEventuallyInReadTransaction(session, "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")), 10L);
        }
    }

    @Test
    public void shouldUpdateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer2', 'RETURN 42 as answer')")); // we create a function
        TestContainerUtil.testCall(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer2', 'RETURN 52 as answer')")); // we update the function

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        TestContainerUtil.testCallEventuallyInReadTransaction(cluster.getSession(), "return custom.answer2() as row", (row) -> assertEquals(52L, ((Map)((List)row.get("row")).get(0)).get("answer")), 10L);
    }

    @Test
    public void shouldRegisterSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure1', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure

        // when
        TestContainerUtil.testCall(cluster.getSession(), "call custom.answerProcedure1()", (row) -> Assert.assertEquals(33L, row.get("answer")));

        // then
        TestContainerUtil.testCallEventuallyInReadTransaction(cluster.getSession(), "call custom.answerProcedure1()", (row) -> Assert.assertEquals(33L, row.get("answer")), 10L);
    }

    @Test
    public void shouldUpdateSimpleStatementOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure2', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure
        TestContainerUtil.testCall(cluster.getSession(), "call custom.answerProcedure2()", (row) -> Assert.assertEquals(33L, row.get("answer")));

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerProcedure2', 'RETURN 55 as answer', 'read', [['answer','long']])")); // we create a procedure

        // then
        TestContainerUtil.testCallEventuallyInReadTransaction(cluster.getSession(), "call custom.answerProcedure2()", (row) -> Assert.assertEquals(55L, row.get("answer")), 10L);
    }

    @Test(expected = DatabaseException.class)
    public void shouldRemoveProcedureOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.asProcedure('answerToRemove', 'RETURN 33 as answer', 'read', [['answer','long']])")); // we create a procedure
        try {
            TestContainerUtil.testCallEventuallyInReadTransaction(cluster.getSession(), "call custom.answerToRemove()", (row) -> Assert.assertEquals(33L, row.get("answer")), 10L);
        } catch (Exception e) {
            fail("Exception while calling the procedure");
        }

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.removeProcedure('answerToRemove')")); // we remove procedure

        // then
        try {
            TestContainerUtil.testCallEventuallyInReadTransaction(cluster.getSession(), "call custom.answerToRemove()", (row) -> fail("Procedure not removed"), 10L);
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
        try {
            TestContainerUtil.testCallEventuallyInReadTransaction(cluster.getSession(), "return custom.answerFunctionToRemove() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")), 10L);
        } catch (Exception e) {
            fail("Exception while calling the function");
        }

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("call apoc.custom.removeFunction('answerFunctionToRemove')")); // we remove procedure

        // then
        try {
            TestContainerUtil.testCallEventuallyInReadTransaction(cluster.getSession(), "return custom.answerFunctionToRemove() as row", (row) -> fail("Function not removed"), 10L);
        } catch (DatabaseException e) {
            String expectedMessage = "Unknown function 'custom.answerFunctionToRemove'";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testRestoreProcedureWorksCorrectlyOnOtherClusterMembers() {
        List<String> listProcNames = IntStream.range(0, 30)
                .mapToObj(i -> "proc" + i)
                .collect(Collectors.toList());

        // for each element, declare a procedure with that name,
        // then call the custom procedure
        // and finally overwrite and re-call it
        listProcNames.forEach(name -> {
            String declareProcedure = String.format("CALL apoc.custom.declareProcedure('%s() :: (answer::INT)', $query)", name);
            String callProcedure = String.format("call custom.%s", name);

            cluster.getSession().writeTransaction(
                    tx -> tx.run(declareProcedure, Map.of("query", "RETURN 42 AS answer"))
            );

            // test that it's work for each node
            cluster.getClusterMembers().forEach(container -> {
                testCallEventually(container.getSession(), callProcedure, Map.of(),
                        row -> assertEquals(42L, row.get("answer")),
                        10L);
            });

            // overwriting on the leader
            cluster.getSession().writeTransaction(
                    tx -> tx.run(declareProcedure, Map.of("query", "RETURN 1 AS answer"))
            );

            // check that it has been updated for each node
            cluster.getClusterMembers().forEach(container -> {
                testCallEventually(container.getSession(), callProcedure, Map.of(),
                        row -> assertEquals(1L, row.get("answer")),
                        10L);
            });
        });
    }

    @Test
    public void testRestoreFunctionWorksCorrectlyOnOtherClusterMembers() {
        // create a list of ["fun1", "fun2", "fun3" ....] strings
        List<String> listFunNames = IntStream.range(0, 30)
                .mapToObj(i -> "fun" + i)
                .collect(Collectors.toList());

        // for each element, declare a function with that name,
        // then call the custom function
        // and finally overwrite and re-call it
        listFunNames.forEach(name -> {
            final String declareFunction = String.format("CALL apoc.custom.declareFunction('%s() :: INT', $query)", name);
            final String funQuery = String.format("return custom.%s() as row", name);

            cluster.getSession().writeTransaction(tx -> tx.run(declareFunction,
                    Map.of("query", "RETURN 42 as answer")
            ));

            // test that it's work each node
            cluster.getClusterMembers().forEach(container -> {
                testCallEventually(container.getSession(), funQuery, Map.of(),
                        row -> assertEquals(42L, row.get("row")),
                        10L);
            });

            // overwriting on the leader
            cluster.getSession().writeTransaction(
                    tx -> tx.run(declareFunction, Map.of("query", "RETURN 1 AS answer"))
            );

            // check that it has been updated for each node
            cluster.getClusterMembers().forEach(container -> {
                testCallEventually(container.getSession(), funQuery, Map.of(),
                        row -> assertEquals(1L, row.get("row")),
                        10L);
            });
        });
    }
}
