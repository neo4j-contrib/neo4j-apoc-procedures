package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static apoc.trigger.Trigger.SYS_NON_LEADER_ERROR;
import static apoc.trigger.TriggerNewProcedures.TRIGGER_NOT_ROUTED_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    // TODO: making sure that a session against "system" can install triggers

    // TODO: making sure that a session against "system" can drop triggers
    
    // TODO: fabric tests

    @Test
    public void testTriggerInstallAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1',{})";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR);
    }

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.add($name, 'RETURN 1',{})";
        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR);
    }

    @Test
    public void testTriggerRemoveAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.remove('neo4j', $name)";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR);
    }

    @Test
    public void testTriggerDropAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.drop('neo4j', $name)";
        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR);
    }

    private static void triggerInSysLeaderMemberCommon(String query, String triggerNotRoutedError) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(4, members.size());
        for (Neo4jContainerExtension container: members) {
            // we skip READ_REPLICA members
            final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
            try (final Session session = container.getSession()) {
                if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || session == null) {
                    continue;
                }
                if (sysIsLeader(session)) {
                    session.run(query, Map.of("name", UUID.randomUUID().toString()));
                } else {
                    try {
                        TestContainerUtil.testCall(session, query,
                                Map.of("name", UUID.randomUUID().toString()),
                                row -> fail("Should fail because of non leader trigger addition"));
                    } catch (RuntimeException e) {
                        String errorMsg = e.getMessage();
                        assertTrue("The actual message is: " + errorMsg, errorMsg.contains(triggerNotRoutedError));
                    }
                }
            }
        }
    }

    private static boolean sysIsLeader(Session session) {
        final String systemRole = TestContainerUtil.singleResultFirstColumn(session, "CALL dbms.cluster.role('system')");
        return "LEADER".equals(systemRole);
    }
}
