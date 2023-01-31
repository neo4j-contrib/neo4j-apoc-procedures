package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import static apoc.trigger.Trigger.SYS_NON_LEADER_ERROR;
import static apoc.trigger.TriggerNewProcedures.TRIGGER_NOT_ROUTED_ERROR;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class TriggerClusterRoutingTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster(3, 1, Collections.emptyMap(), Map.of(
                        "NEO4J_dbms_routing_enabled", "true",
                        "apoc.trigger.enabled", "true"
                ));
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }
    
    // TODO: fabric tests once the @SystemOnlyProcedure annotation is added to Neo4j

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.add($name, 'RETURN 1', {})";
        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR, DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerRemoveAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.remove($name)";
        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR, DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerInstallAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testTriggerDropAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.drop('neo4j', $name)";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME, 
                (session, name) -> testCallEmpty(session, query, Map.of("name", name)));
    }

    private static void triggerInSysLeaderMemberCommon(String query, String triggerNotRoutedError, String dbName) {
        final BiConsumer<Session, String> testTrigger = (session, name) -> testCall(session, query,
                Map.of("name", name),
                row -> assertEquals(name, row.get("name")));
        triggerInSysLeaderMemberCommon(query, triggerNotRoutedError, dbName, testTrigger);
    }

    private static void triggerInSysLeaderMemberCommon(String query, String triggerNotRoutedError, String dbName, BiConsumer<Session, String> testTrigger) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(4, members.size());
        for (Neo4jContainerExtension container: members) {
            // we skip READ_REPLICA members
            final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
            final Driver driver = container.getDriver();
            if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || driver == null) {
                continue;
            }
            Session session = driver.session(SessionConfig.forDatabase(dbName));
            if (sysIsLeader(session)) {
                final String name = UUID.randomUUID().toString();
                testTrigger.accept(session, name);
            } else {
                try {
                    testCall(session, query,
                            Map.of("name", UUID.randomUUID().toString()),
                            row -> fail("Should fail because of non leader trigger addition"));
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(triggerNotRoutedError));
                }
            }
        }
    }

    private static boolean sysIsLeader(Session session) {
        final String systemRole = TestContainerUtil.singleResultFirstColumn(session, "CALL dbms.cluster.role('system')");
        return "LEADER".equals(systemRole);
    }


}
