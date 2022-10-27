package apoc.trigger;

import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class TriggerNewProceduresClusterTest {

    private static TestcontainersCausalCluster cluster;
    private static Session session;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil.createEnterpriseCluster(3, 1, 
                Collections.emptyMap(),
                Map.of(
                        "apoc.trigger.refresh", "100",
                        "apoc.trigger.enabled", "true"
                ));
        session = cluster.getSession();
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

    @Before
    public void before() {
        session.run("CALL apoc.trigger.dropAll('neo4j')");
        session.run("MATCH (n) DETACH DELETE n");
    }

    //
    // test cases taken and adapted from TriggerClusterTest.java
    //

    @Test
    public void testTimeStampTriggerForUpdatedProperties() throws Exception {
        session.run("CALL apoc.trigger.install('neo4j', 'timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        session.run("CREATE (f:Foo) SET f.foo='bar'");
        TestContainerUtil.testCall(cluster.getSession(), "MATCH (f:Foo) RETURN f", (row) -> {
            assertTrue(((Node) row.get("f")).containsKey("ts"));
        });
    }

    @Test
    public void testReplication() {
        session.run("CALL apoc.trigger.install('neo4j', 'timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        // Test that the trigger is present in another instance
        awaitProcedureUpdated(cluster.getDriver().session(), "timestamp");
    }

    @Test
    public void testLowerCaseName() {
        session.run("create constraint on (p:Person) assert p.id is unique");
        session.run("CALL apoc.trigger.install('neo4j', 'lowercase', 'UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n.id = toLower(n.name)',{})");

        awaitProcedureUpdated(session, "lowercase");

        cluster.getSession().run("CREATE (f:Person {name:'John Doe'})");
        TestContainerUtil.testCall(session, "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node)row.get("f")).get("id").asString());
            assertEquals("John Doe", ((Node)row.get("f")).get("name").asString());
        });
    }

    @Test
    public void testSetLabels() {
        session.run("CREATE (f:Test {name:'John Doe'})");
        session.run("CALL apoc.trigger.install('neo4j', 'setlabels','UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n:Man',{})");
        
        awaitProcedureUpdated(session, "setlabels");

        session.run("MATCH (f:Test) SET f:Person");
        TestContainerUtil.testCall(session, "MATCH (f:Man) RETURN f", (row) -> {
            assertEquals("John Doe", ((Node)row.get("f")).get("name").asString());
            assertTrue(((Node) row.get("f")).hasLabel("Person"));
        });

        long count = TestContainerUtil.singleResultFirstColumn(session, "MATCH (f:Man) RETURN count(*) as c");
        assertEquals(1L, count);
    }

    @Test
    public void testTxIdAfterAsync() {
        session.run("CALL apoc.trigger.install('neo4j', 'triggerTest','UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, \"_executed\") as prop " +
                "	WITH prop.node as n " +
                "	CREATE (z:SON {father:id(n)}) " +
                "	CREATE (n)-[:GENERATED]->(z)', " +
                "{phase:'afterAsync'})");
        
        awaitProcedureUpdated(session, "triggerTest");

        session.run("CREATE (:TEST {name:'x', _executed:0})");
        session.run("CREATE (:TEST {name:'y', _executed:0})");
        assertEventually(() -> TestContainerUtil.<Long>singleResultFirstColumn(session, "MATCH p = ()-[r:GENERATED]->() RETURN count(p) AS count"),
                (value) -> value == 2L, 30, TimeUnit.SECONDS);
    }

    private static void awaitProcedureUpdated(Session session, String name) {
        assertEventually(() -> session
                        .readTransaction(tx -> tx.run("CALL apoc.trigger.list")
                                .single().get("name").asString()),
                name::equals,
                30, TimeUnit.SECONDS);
    }
}
