package apoc.trigger;

import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.types.Node;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class TriggerClusterTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() ->  cluster = TestContainerUtil
                .createEnterpriseCluster(3, 1, Collections.emptyMap(), MapUtil.stringMap(
                        "apoc.trigger.refresh", "100",
                        "apoc.trigger.enabled", "true"
                )),
                Exception.class);
        Assume.assumeNotNull(cluster);
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Before
    public void before() {
        cluster.getSession().run("CALL apoc.trigger.removeAll()");
        cluster.getSession().run("MATCH (n) DETACH DELETE n");
    }


    @Test
    public void testTimeStampTriggerForUpdatedProperties() throws Exception {
        cluster.getSession().run("CALL apoc.trigger.add('timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        cluster.getSession().run("CREATE (f:Foo) SET f.foo='bar'");
        TestContainerUtil.testCall(cluster.getSession(), "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node)row.get("f")).containsKey("ts"));
        });
    }

    @Test
    public void testReplication() throws Exception {
        cluster.getSession().run("CALL apoc.trigger.add('timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        // Test that the trigger is present in another instance
        org.neo4j.test.assertion.Assert.assertEventually(() -> cluster.getDriver().session()
                        .readTransaction(tx -> tx.run("CALL apoc.trigger.list() YIELD name RETURN name").single().get("name").asString()),
                (value) -> "timestamp".equals(value), 30, TimeUnit.SECONDS);
    }

    @Test
    public void testLowerCaseName() throws Exception {
        cluster.getSession().run("create constraint on (p:Person) assert p.id is unique");
        cluster.getSession().run("CALL apoc.trigger.add('lowercase','UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n.id = toLower(n.name)',{})");
        cluster.getSession().run("CREATE (f:Person {name:'John Doe'})");
        TestContainerUtil.testCall(cluster.getSession(), "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node)row.get("f")).get("id").asString());
            assertEquals("John Doe", ((Node)row.get("f")).get("name").asString());
        });
    }
    @Test
    public void testSetLabels() throws Exception {
        cluster.getSession().run("CREATE (f {name:'John Doe'})");
        cluster.getSession().run("CALL apoc.trigger.add('setlabels','UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n:Man',{})");
        cluster.getSession().run("MATCH (f) SET f:Person");
        TestContainerUtil.testCall(cluster.getSession(), "MATCH (f:Man) RETURN f", (row) -> {
            assertEquals("John Doe", ((Node)row.get("f")).get("name").asString());
            assertEquals(true, ((Node)row.get("f")).hasLabel("Person"));
        });

        long count = TestContainerUtil.singleResultFirstColumn(cluster.getSession(), "MATCH (f:Man) RETURN count(*) as c");
        assertEquals(1L, count);
    }


    @Test
    public void testTxIdAfterAsync() throws Exception {
        cluster.getSession().run("CALL apoc.trigger.add('triggerTest','UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, \"_executed\") as prop " +
                "	WITH prop.node as n " +
                "	CREATE (z:SON {father:id(n)}) " +
                "	CREATE (n)-[:GENERATED]->(z)', " +
                "{phase:'afterAsync'})");
        cluster.getSession().run("CREATE (:TEST {name:'x', _executed:0})");
        cluster.getSession().run("CREATE (:TEST {name:'y', _executed:0})");
        org.neo4j.test.assertion.Assert.assertEventually(() -> TestContainerUtil.<Long>singleResultFirstColumn(cluster.getSession(), "MATCH p = ()-[r:GENERATED]->() RETURN count(p) AS count"),
                (value) -> value == 2L, 30, TimeUnit.SECONDS);
    }
}
