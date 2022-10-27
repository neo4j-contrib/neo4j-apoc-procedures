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

    @Test
    public void testTriggerAddAndInstallAllowedOnlyInLeaderMember() {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(4, members.size());
        for (Neo4jContainerExtension container: members){
            // we skip READ_REPLICA members
            final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
            final Session session = container.getSession();
            if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || session == null) {
                continue;
            }
            final String systemRole = TestContainerUtil.singleResultFirstColumn(session, "CALL dbms.cluster.role('system')");
            if ("LEADER".equals(systemRole)) {
                session.run("CALL apoc.trigger.add($name, 'RETURN 1',{})",
                        Map.of("name", "add-" + container.getContainerName()));
                
                session.run("CALL apoc.trigger.install('neo4j', $name, 'RETURN 1',{})",
                        Map.of("name", "install-" + container.getContainerName()));
            } else {
                try {
                    TestContainerUtil.testCall(session, "CALL apoc.trigger.add($name, 'RETURN 1',{})",
                            Map.of("name", "add-" + container.getContainerName()),
                            row -> fail("Should fail because of non leader trigger addition"));
                } catch (RuntimeException e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(SYS_NON_LEADER_ERROR));
                }
                try {
                    TestContainerUtil.testCall(session, "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1',{})",
                            Map.of("name", "install-" + container.getContainerName()),
                            row -> fail("Should fail because of non leader trigger addition"));
                } catch (RuntimeException e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(TRIGGER_NOT_ROUTED_ERROR));
                }
            }
        }
    }
}
