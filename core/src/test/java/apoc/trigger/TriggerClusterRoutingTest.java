package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.types.Node;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.trigger.TriggerDeprecatedProcedures.SYS_NON_LEADER_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TriggerClusterRoutingTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster(3, 1, Collections.emptyMap(), MapUtil.stringMap(
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
    public void testTriggerAddAllowedOnlyInLeaderMember() {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(4, members.size());
        for (Neo4jContainerExtension container: members){
            // we skip READ_REPLICA members
            final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
            if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || container.getSession() == null) {
                continue;
            }
            final String systemRole = TestContainerUtil.singleResultFirstColumn(container.getSession(), "CALL dbms.cluster.role('system')");
            if ("LEADER".equals(systemRole)) {
                container.getSession().run("CALL apoc.trigger.add($name, 'UNWIND $createdNodes AS n SET n.ts = timestamp()',{})",
                        Map.of("name", "trigger-" + container.getContainerName()));

                container.getSession().run("CREATE (f:Foo)");
                TestContainerUtil.testCall(container.getSession(), "MATCH (f:Foo) RETURN f",
                        (row) -> assertTrue(((Node) row.get("f")).containsKey("ts")));
            } else {
                try {
                    TestContainerUtil.testCall(container.getSession(), "CALL apoc.trigger.add($name, 'UNWIND $createdNodes AS n SET n.ts = timestamp()',{})",
                            Map.of("name", "trigger-" + container.getContainerName()),
                            row -> fail("Should fail because of non leader trigger addition"));
                } catch (RuntimeException e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(SYS_NON_LEADER_ERROR));
                }
            }
        }
    }
}
