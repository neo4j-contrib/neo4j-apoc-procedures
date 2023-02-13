package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.ExtendedTestContainerUtil;
import apoc.util.TestContainerUtil;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.Session;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class TriggerClusterTest {

    private static TestcontainersCausalCluster cluster;
    private static Driver writeDriver;
    private static Session neo4jSession;
    private static Session systemSession;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil.createEnterpriseCluster(
                List.of(ApocPackage.CORE, ApocPackage.EXTENDED),
                3,
                1,
                Collections.emptyMap(),
                Map.of("NEO4J_dbms_routing_enabled", "true",
                        "apoc.trigger.refresh", "100", 
                        "apoc.trigger.enabled", "true"));

        writeDriver = getReadWriteDrivers();
        neo4jSession = writeDriver.session();
        systemSession = writeDriver.session(forDatabase(SYSTEM_DATABASE_NAME));
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

    @Before
    public void before() {
        neo4jSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.removeAll()"));
        neo4jSession.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n"));
    }

    @Test
    public void testTimeStampTriggerForUpdatedProperties() throws Exception {
        neo4jSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.add('timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})"));
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (f:Foo) SET f.foo='bar'"));
        TestContainerUtil.testCall(neo4jSession, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node)row.get("f")).containsKey("ts"));
        });
    }

    @Test
    public void testReplication() throws Exception {
        neo4jSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.add('timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})"));
        // Test that the trigger is present in another instance
        org.neo4j.test.assertion.Assert.assertEventually(() -> cluster.getSession()
                        .readTransaction(tx -> tx.run("CALL apoc.trigger.list() YIELD name RETURN name").single().get("name").asString()),
                (value) -> "timestamp".equals(value), 30, TimeUnit.SECONDS);
    }

    @Test
    public void testLowerCaseName() throws Exception {
        neo4jSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.add('lowercase','UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n.id = toLower(n.name)',{})"));
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (f:Person {name:'John Doe'})"));
        TestContainerUtil.testCall(neo4jSession, "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node)row.get("f")).get("id").asString());
            assertEquals("John Doe", ((Node)row.get("f")).get("name").asString());
        });
    }

    @Test
    public void testSetLabels() throws Exception {
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (f {name:'John Doe'})"));
        neo4jSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.add('setlabels','UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n:Man',{})"));
        neo4jSession.writeTransaction(tx -> tx.run("MATCH (f) SET f:Person"));
        TestContainerUtil.testCall(neo4jSession, "MATCH (f:Man) RETURN f", (row) -> {
            assertEquals("John Doe", ((Node)row.get("f")).get("name").asString());
            assertEquals(true, ((Node)row.get("f")).hasLabel("Person"));
        });

        long count = ExtendedTestContainerUtil.singleResultFirstColumn(neo4jSession, "MATCH (f:Man) RETURN count(*) as c");
        assertEquals(1L, count);
    }

    @Test
    public void testTxIdAfterAsync() throws Exception {
        neo4jSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.add('triggerTest','UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, \"_executed\") as prop " +
                "	WITH prop.node as n " +
                "	CREATE (z:SON {father:id(n)}) " +
                "	CREATE (n)-[:GENERATED]->(z)', " +
                "{phase:'afterAsync'})"));
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (:TEST {name:'x', _executed:0})"));
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (:TEST {name:'y', _executed:0})"));
        org.neo4j.test.assertion.Assert.assertEventually(() -> ExtendedTestContainerUtil.<Long>singleResultFirstColumn(neo4jSession, "MATCH p = ()-[r:GENERATED]->() RETURN count(p) AS count"),
                (value) -> value == 2L, 30, TimeUnit.SECONDS);
    }

    //
    // test cases duplicated, regarding new procedures
    //

    @Test
    public void testTimeStampTriggerForUpdatedPropertiesNewProcedures() throws Exception {
        final String name = "timestampUpdate";
        systemSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.install('neo4j', $name,'UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})",
                Map.of("name", name)));
        awaitProcedureInstalled(neo4jSession, "timestampUpdate");
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (f:Foo) SET f.foo='bar'"));
        TestContainerUtil.testCall(neo4jSession, "MATCH (f:Foo) RETURN f", (row) -> {
            assertTrue(((Node) row.get("f")).containsKey("ts"));
        });
    }

    @Test
    public void testReplicationNewProcedures() throws Exception {
        systemSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.install('neo4j', 'timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})"));
        // Test that the trigger is present in another instance
        awaitProcedureInstalled(cluster.getSession(), "timestamp");
    }

    @Test
    public void testLowerCaseNameNewProcedures() {
        final String name = "lowercase";
        systemSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.install('neo4j', $name, 'UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n.id = toLower(n.name)',{})",
                Map.of("name", name)));
        neo4jSession.writeTransaction(tx -> tx.run("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE"));
        awaitProcedureInstalled(cluster.getSession(), name);
        
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (f:Person {name:'John Doe'})"));
        TestContainerUtil.testCall(writeDriver.session(), "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node) row.get("f")).get("id").asString());
            assertEquals("John Doe", ((Node) row.get("f")).get("name").asString());
        });
    }

    @Test
    public void testSetLabelsNewProcs() throws Exception {
        final String name = "testSetLabels";
        systemSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.install('neo4j', $name,'UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n:Man',{})",
                Map.of("name", name)));
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (f:Test {name:'John Doe'})"));

        awaitProcedureInstalled(neo4jSession, name);

        neo4jSession.writeTransaction(tx -> tx.run("MATCH (f:Test) SET f:Person"));
        TestContainerUtil.testCall(neo4jSession, "MATCH (f:Man) RETURN f", (row) -> {
            assertEquals("John Doe", ((Node) row.get("f")).get("name").asString());
            assertTrue(((Node) row.get("f")).hasLabel("Person"));
        });

        long count = ExtendedTestContainerUtil.singleResultFirstColumn(neo4jSession, "MATCH (f:Man) RETURN count(*) as c");
        assertEquals(1L, count);
    }

    @Test
    public void testTxIdAfterAsyncNewProcedures() throws Exception {
        final String name = "testTxIdAfterAsync";
        systemSession.writeTransaction(tx -> tx.run("CALL apoc.trigger.install('neo4j', $name, 'UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, \"_executed\") as prop " +
                        "	WITH prop.node as n " +
                        "	CREATE (z:SON {father:id(n)}) " +
                        "	CREATE (n)-[:GENERATED]->(z)', " +
                        "{phase:'afterAsync'})",
                Map.of("name", name)));
            
        awaitProcedureInstalled(neo4jSession, name);

        neo4jSession.writeTransaction(tx -> tx.run("CREATE (:TEST {name:'x', _executed:0})"));
        neo4jSession.writeTransaction(tx -> tx.run("CREATE (:TEST {name:'y', _executed:0})"));
        assertEventually(() -> ExtendedTestContainerUtil.<Long>singleResultFirstColumn(neo4jSession, "MATCH p = ()-[r:GENERATED]->() RETURN count(p) AS count"),
                (value) -> value == 2L, 30, TimeUnit.SECONDS);
    }

    private static Driver getReadWriteDrivers() {
        // get the 1st system write driver found
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        System.out.println("membersSize = " + members.size());
        System.out.println("members = " + members);
        for (Neo4jContainerExtension container: members) {
            final Driver driver = container.getDriver();
            if (driver == null) {
                continue;
            }
            System.out.println("co = " + container.getEnvMap());
            final String address = container.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
            final boolean isSysWriter = dbIsWriter(driver, address);
            if (isSysWriter) {
                return driver;
            }
        }
        return null;
    }

    private static boolean dbIsWriter(Driver driver, String address) {
        final Session session = driver.session(forDatabase(SYSTEM_DATABASE_NAME));
        System.out.println("address = " + address);
        
        return session.run( "SHOW DATABASE $dbName WHERE address = $address",
                        Map.of("dbName", SYSTEM_DATABASE_NAME, "address", address) )
                .single()
                .get("writer")
                .asBoolean();
    }

    private static void awaitProcedureInstalled(Session session, String name) {
        assertEventually(() -> session
                        .readTransaction(tx -> tx.run("CALL apoc.trigger.list")
                                .single().get("name").asString()),
                name::equals,
                30, TimeUnit.SECONDS);
    }
}
