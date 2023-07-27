package apoc.custom;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import apoc.util.collection.Iterators;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static apoc.custom.CypherProceduresHandler.CUSTOM_PROCEDURES_REFRESH;
import static apoc.util.ExtendedTestContainerUtil.singleResultFirstColumn;
import static apoc.util.ExtendedTestContainerUtil.dbIsWriter;
import static apoc.util.ExtendedTestContainerUtil.getBoltAddress;
import static apoc.util.ExtendedTestContainerUtil.getDriverIfNotReplica;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.ExtendedTestContainerUtil.routingSessionForEachMembers;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static apoc.util.TestContainerUtil.testResult;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CypherProceduresClusterRoutingTest {
    private static final int NUM_CORES = 3;
    private static TestcontainersCausalCluster cluster;
    private static Session clusterSession;
    private static List<Neo4jContainerExtension> members;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster(List.of(TestContainerUtil.ApocPackage.EXTENDED),
                        NUM_CORES, 0,
                        Collections.emptyMap(),
                        Map.of("NEO4J_initial_server_mode__constraint", "PRIMARY",
                                "NEO4J_dbms_routing_enabled", "true",
                                CUSTOM_PROCEDURES_REFRESH, "1000"
                        ));

        clusterSession = cluster.getSession();
        members = cluster.getClusterMembers();

        assertEquals(NUM_CORES, members.size());
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }


    @Test
    public void testSetupAndDropCustomsWithUseSystemClause() {

        // create a custom procedure and function for each member
        routingSessionForEachMembers(members, (session, container) -> {
            String name = getUniqueName(container);
            String statement = "RETURN 42 AS answer";

            final String queryProc = "CALL apoc.custom.installProcedure($signature, $statement)";
            Map<String, Object> paramsProc = Map.of("signature", name + "() :: (answer::ANY)",
                    "statement", statement);
            session.writeTransaction(tx -> tx.run(queryProc, paramsProc));

            final String queryFun = "CALL apoc.custom.installFunction($signature, $statement)";
            Map<String, Object> paramsFun = Map.of("signature", name + "() :: STRING",
                    "statement", statement);
            session.writeTransaction(tx -> tx.run(queryFun, paramsFun));
        });

        // the apoc.custom.list count is equal to 2 items for each member (1 proc. and 1 fun.)
        String countCustom = "CALL apoc.custom.list() YIELD name RETURN count(*) AS count";
        int expectedCount = members.size() * 2;
        assertEventually(() -> (long) singleResultFirstColumn(clusterSession, countCustom),
                (value) -> value == expectedCount, TIMEOUT, SECONDS);

        // check that every custom proc/fun is correctly installed and propagated to other members
        List<String> customNames = members.stream()
                .map(CypherProceduresClusterRoutingTest::getUniqueName)
                .toList();
        for (Neo4jContainerExtension member : members) {
            Session session = member.getSession();

            for (String name: customNames) {
                assertEventually(() -> (long) singleResultFirstColumn(session, "CALL custom.%s".formatted(name)),
                        (v) -> v == 42L, TIMEOUT, SECONDS);

                assertEventually(() -> (long) singleResultFirstColumn(session, "RETURN custom.%s() AS answer".formatted(name)),
                        (v) -> v == 42L, TIMEOUT, SECONDS);
            }
        }

        // drop the previous custom procedures/functions
        routingSessionForEachMembers(members, (session, container) -> {
            String name = getUniqueName(container);
            Map<String, Object> params = Map.of("name", name);

            String queryProc = "CALL apoc.custom.dropProcedure($name)";
            session.writeTransaction(tx -> tx.run(queryProc, params));

            String queryFun = "CALL apoc.custom.dropFunction($name)";
            session.writeTransaction(tx -> tx.run(queryFun, params));
        });

        assertEventually(() -> (long) singleResultFirstColumn(clusterSession, countCustom),
                (value) -> value == 0L, 20L, SECONDS);
    }

    @Test
    public void testCustomInstallAndDropAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.custom.installProcedure($name, 'RETURN 42 AS answer')";
        final String queryFun = "CALL apoc.custom.installFunction($name, 'RETURN 42 AS answer')";
        customInSysLeaderMemberCommon(
                (session, name) -> {
                    // create customs
                    Map<String, Object> paramsProc = Map.of("name", name + "() :: (answer::ANY)");
                    testCallEmpty(session, query, paramsProc);

                    Map<String, Object> paramsFun = Map.of("name", name + "() :: INT");
                    testCallEmpty(session, queryFun, paramsFun);

                    // drop customs
                    String queryDropProc = "CALL apoc.custom.dropProcedure($name)";
                    testCallEmpty(session, queryDropProc, paramsProc);

                    String queryDropFun = "CALL apoc.custom.dropFunction($name)";
                    testCallEmpty(session, queryDropFun, paramsProc);
                }
        );
    }

    @Test
    public void testCustomShowAllowedInAllSysLeaderMembers() {
        final String query = "CALL apoc.custom.show";
        final BiConsumer<Session, String> testUuidShow = (session, name) -> testResult(session, query, Iterators::count);
        customInSysLeaderMemberCommon(testUuidShow, true);
    }


    private static void customInSysLeaderMemberCommon(BiConsumer<Session, String> testUuid) {
        customInSysLeaderMemberCommon(testUuid, false);
    }

    private static void customInSysLeaderMemberCommon(BiConsumer<Session, String> testUuid, boolean readOnlyOperation) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(NUM_CORES, members.size());
        for (Neo4jContainerExtension container: members) {
            final String name = getUniqueName(container);
            // we skip READ_REPLICA members with readOnlyOperation=false
            final Driver driver = readOnlyOperation
                    ? container.getDriver()
                    : getDriverIfNotReplica(container);
            if (driver == null) {
                continue;
            }
            Session session = driver.session(SessionConfig.forDatabase(SYSTEM_DATABASE_NAME));
            boolean isWriter = dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(container));
            if (readOnlyOperation || isWriter) {
                testUuid.accept(session, name);
            } else {
                try {
                    testUuid.accept(session, name);
                    fail("Should fail because of non leader custom procedure/function addition");
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(apoc.util.SystemDbUtil.PROCEDURE_NOT_ROUTED_ERROR));
                }
            }
        }
    }

    private static String getUniqueName(Neo4jContainerExtension container) {
        return "proc" + container.getMappedPort(7687).toString();
    }
}