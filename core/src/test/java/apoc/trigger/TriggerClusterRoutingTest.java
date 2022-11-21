package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

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
    public void testTriggerAddAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.add($name, 'RETURN 1', {})";
        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR, GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerRemoveAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.remove($name)";
        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR, GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerInstallAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR, GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testTriggerDropAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.drop('neo4j', $name)";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR, GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    private static void triggerInSysLeaderMemberCommon(String query, String triggerNotRoutedError, String dbName) {
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
            session.writeTransaction(tx -> {
                if (sysIsLeader(tx)) {
                    tx.run(query, Map.of("name", UUID.randomUUID().toString())).consume();
                } else {
                    try {
                        tx.run(query, Map.of("name", UUID.randomUUID().toString())).consume();
                        fail("Should fail because of non leader trigger addition");
                    } catch (Exception e) {
                        String errorMsg = e.getMessage();
                        assertTrue("The actual message is: " + errorMsg, errorMsg.contains(triggerNotRoutedError));
                    }
                }
                return null;
            });
        }
    }

    private static boolean sysIsLeader(Transaction tx) {
        final String systemRole = tx.run("CALL dbms.cluster.role('system')")
                .single().get("role").asString();
        return "LEADER".equals(systemRole);
    }
}
