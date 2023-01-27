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
    private static Driver readDriver;

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

        getReadWriteDrivers();
        System.out.println("getReadWriteDrivers()");
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

    @Before
    public void before() {
        System.out.println("writeDriver = " + writeDriver);
        writeDriver.session().run("CALL apoc.trigger.removeAll()");
        writeDriver.session().run("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testTimeStampTriggerForUpdatedProperties() throws Exception {
        writeDriver.session().run("CALL apoc.trigger.add('timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        writeDriver.session().run("CREATE (f:Foo) SET f.foo='bar'");
        TestContainerUtil.testCall(writeDriver.session(), "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node)row.get("f")).containsKey("ts"));
        });
    }

    @Test
    public void testReplication() throws Exception {
        writeDriver.session().run("CALL apoc.trigger.add('timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        // Test that the trigger is present in another instance
        org.neo4j.test.assertion.Assert.assertEventually(() -> readDriver.session()
                        .readTransaction(tx -> tx.run("CALL apoc.trigger.list() YIELD name RETURN name").single().get("name").asString()),
                (value) -> "timestamp".equals(value), 30, TimeUnit.SECONDS);
    }

    @Test
    public void testLowerCaseName() throws Exception {
        writeDriver.session().run("create constraint on (p:Person) assert p.id is unique");
        writeDriver.session().run("CALL apoc.trigger.add('lowercase','UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n.id = toLower(n.name)',{})");
        writeDriver.session().run("CREATE (f:Person {name:'John Doe'})");
        TestContainerUtil.testCall(writeDriver.session(), "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node)row.get("f")).get("id").asString());
            assertEquals("John Doe", ((Node)row.get("f")).get("name").asString());
        });
    }

    @Test
    public void testSetLabels() throws Exception {
        writeDriver.session().run("CREATE (f {name:'John Doe'})");
        writeDriver.session().run("CALL apoc.trigger.add('setlabels','UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n:Man',{})");
        writeDriver.session().run("MATCH (f) SET f:Person");
        TestContainerUtil.testCall(writeDriver.session(), "MATCH (f:Man) RETURN f", (row) -> {
            assertEquals("John Doe", ((Node)row.get("f")).get("name").asString());
            assertEquals(true, ((Node)row.get("f")).hasLabel("Person"));
        });

        long count = ExtendedTestContainerUtil.singleResultFirstColumn(writeDriver.session(), "MATCH (f:Man) RETURN count(*) as c");
        assertEquals(1L, count);
    }

    @Test
    public void testTxIdAfterAsync() throws Exception {
        writeDriver.session().run("CALL apoc.trigger.add('triggerTest','UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, \"_executed\") as prop " +
                "	WITH prop.node as n " +
                "	CREATE (z:SON {father:id(n)}) " +
                "	CREATE (n)-[:GENERATED]->(z)', " +
                "{phase:'afterAsync'})");
        writeDriver.session().run("CREATE (:TEST {name:'x', _executed:0})");
        writeDriver.session().run("CREATE (:TEST {name:'y', _executed:0})");
        org.neo4j.test.assertion.Assert.assertEventually(() -> ExtendedTestContainerUtil.<Long>singleResultFirstColumn(writeDriver.session(), "MATCH p = ()-[r:GENERATED]->() RETURN count(p) AS count"),
                (value) -> value == 2L, 30, TimeUnit.SECONDS);
    }

    //
    // test cases duplicated, regarding new procedures
    //

    @Test
    public void testTimeStampTriggerForUpdatedPropertiesNewProcedures() throws Exception {
        final String name = "timestampUpdate";
        try (final Session session = writeDriver.session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install('neo4j', $name,'UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})",
                    Map.of("name", name));
        }
        try (final Session session = writeDriver.session()) {
            awaitProcedureInstalled(session, "timestampUpdate");
            session.run("CREATE (f:Foo) SET f.foo='bar'");
            TestContainerUtil.testCall(session, "MATCH (f:Foo) RETURN f", (row) -> {
                assertTrue(((Node) row.get("f")).containsKey("ts"));
            });
        }
    }

    @Test
    public void testReplicationNewProcedures() throws Exception {
        try (final Session session = writeDriver.session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install('neo4j', 'timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        }
        // Test that the trigger is present in another instance
        awaitProcedureInstalled(readDriver.session(), "timestamp");
    }

    @Test
    public void testLowerCaseNameNewProcedures() {
        final String name = "lowercase";
        writeDriver.session(forDatabase(SYSTEM_DATABASE_NAME)).run("CALL apoc.trigger.install('neo4j', $name, 'UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n.id = toLower(n.name)',{})",
                Map.of("name", name));
        writeDriver.session().run("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE");
        awaitProcedureInstalled(cluster.getSession(), name);

        writeDriver.session().run("CREATE (f:Person {name:'John Doe'})");
        TestContainerUtil.testCall(writeDriver.session(), "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node) row.get("f")).get("id").asString());
            assertEquals("John Doe", ((Node) row.get("f")).get("name").asString());
        });
    }

    @Test
    public void testSetLabelsNewProcs() throws Exception {
        final String name = "testSetLabels";
        try (final Session session = writeDriver.session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install('neo4j', $name,'UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n:Man',{})",
                    Map.of("name", name));
        }
        try (final Session session = writeDriver.session()) {
            session.run("CREATE (f:Test {name:'John Doe'})");

            awaitProcedureInstalled(session, name);

            session.run("MATCH (f:Test) SET f:Person");
            TestContainerUtil.testCall(session, "MATCH (f:Man) RETURN f", (row) -> {
                assertEquals("John Doe", ((Node) row.get("f")).get("name").asString());
                assertTrue(((Node) row.get("f")).hasLabel("Person"));
            });

            long count = ExtendedTestContainerUtil.singleResultFirstColumn(session, "MATCH (f:Man) RETURN count(*) as c");
            assertEquals(1L, count);
        }
    }

    @Test
    public void testTxIdAfterAsyncNewProcedures() throws Exception {
        final String name = "testTxIdAfterAsync";
        try (final Session session = writeDriver.session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install('neo4j', $name, 'UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, \"_executed\") as prop " +
                            "	WITH prop.node as n " +
                            "	CREATE (z:SON {father:id(n)}) " +
                            "	CREATE (n)-[:GENERATED]->(z)', " +
                            "{phase:'afterAsync'})",
                    Map.of("name", name));
        }
        try (final Session session = writeDriver.session()) {
            awaitProcedureInstalled(session, name);

            session.run("CREATE (:TEST {name:'x', _executed:0})");
            session.run("CREATE (:TEST {name:'y', _executed:0})");
            assertEventually(() -> ExtendedTestContainerUtil.<Long>singleResultFirstColumn(session, "MATCH p = ()-[r:GENERATED]->() RETURN count(p) AS count"),
                    (value) -> value == 2L, 30, TimeUnit.SECONDS);
        }
    }

    private static void getReadWriteDrivers() {
        // get the 1st system write driver found
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        for (Neo4jContainerExtension container: members) {
            if (writeDriver != null || readDriver != null) {
                return;
            }
            final Driver driver = container.getDriver();
            // TODO - remove...
            System.out.println("container.getEnvMap().get(\"NEO4J_dbms_mode\") = " + container.getEnvMap().get("NEO4J_dbms_mode"));
            if (driver == null) {
                continue;
            }
            final String address = container.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
            final boolean isSysWriter = dbIsWriter(driver, address);
            if (isSysWriter) {
                writeDriver = driver;
            }
            readDriver = driver;
        }
        if (writeDriver == null) {
            throw new RuntimeException("No read member found");    
        }
        throw new RuntimeException("No write member found");
    }

    private static boolean dbIsWriter(Driver driver, String address) {
        final Session session = driver.session(forDatabase(SYSTEM_DATABASE_NAME));

        // TODO - to debug, remove... 
        System.out.println("address = " + address);
//        final List<Map<String, Object>> showDatabase = session.run("SHOW DATABASE").stream().map(Record::asMap).collect(Collectors.toList());
//        System.out.println("showDatabase = " + showDatabase);

        return session.run( "SHOW DATABASE $dbName WHERE address = $address",
                        Map.of("dbName", SYSTEM_DATABASE_NAME, "address", address) )
                .single().get("writer")
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
