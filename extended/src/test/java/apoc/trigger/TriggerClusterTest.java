package apoc.trigger;

import apoc.util.TestContainerUtil;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.Session;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class TriggerClusterTest {

    private static final String DB_FOO = "foo";
    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil.createEnterpriseCluster(
                // TODO [Nacho] We cannot build core from here anymore. This needs rethinking
                List.of(ApocPackage.CORE, ApocPackage.EXTENDED),
                3,
                1,
                Collections.emptyMap(),
                Map.of("apoc.trigger.refresh", "100", "apoc.trigger.enabled", "true"));
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
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


    //
    // test cases duplicated, regarding new procedures
    //

    @Test
    public void testTimeStampTriggerForUpdatedPropertiesNewProcedures() throws Exception {
        final String name = "timestampUpdate";
        try (final Session session = cluster.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install('neo4j', $name,'UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})",
                    Map.of("name", name));
        }
        try (final Session session = cluster.getSession()) {
            awaitProcedureInstalled(session, "timestampUpdate");
            session.run("CREATE (f:Foo) SET f.foo='bar'");
            TestContainerUtil.testCall(session, "MATCH (f:Foo) RETURN f", (row) -> {
                assertTrue(((Node) row.get("f")).containsKey("ts"));
            });
        }
    }

    @Test
    public void testReplicationNewProcedures() throws Exception {
        try (final Session session = cluster.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install('neo4j', 'timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        }
        // Test that the trigger is present in another instance
        awaitProcedureInstalled(cluster.getDriver().session(), "timestamp");
    }

    @Test
    public void testLowerCaseNameNewProcedures() {
        final String name = "lowercase";
        try (final Session session = cluster.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install('neo4j', $name, 'UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n.id = toLower(n.name)',{})",
                    Map.of("name", name));
        }
        try (final Session session = cluster.getSession()) {
            session.run("create constraint on (p:Person) assert p.id is unique");
            awaitProcedureInstalled(session, name);

            session.run("CREATE (f:Person {name:'John Doe'})");
            TestContainerUtil.testCall(session, "MATCH (f:Person) RETURN f", (row) -> {
                assertEquals("john doe", ((Node) row.get("f")).get("id").asString());
                assertEquals("John Doe", ((Node) row.get("f")).get("name").asString());
            });
        }
    }

    @Test
    public void testSetLabelsNewProcs() throws Exception {
        final String name = "testSetLabels";
        try (final Session session = cluster.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install('neo4j', $name,'UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n:Man',{})",
                    Map.of("name", name));
        }
        try (final Session session = cluster.getSession()) {
            session.run("CREATE (f:Test {name:'John Doe'})");

            awaitProcedureInstalled(session, name);

            session.run("MATCH (f:Test) SET f:Person");
            TestContainerUtil.testCall(session, "MATCH (f:Man) RETURN f", (row) -> {
                assertEquals("John Doe", ((Node) row.get("f")).get("name").asString());
                assertTrue(((Node) row.get("f")).hasLabel("Person"));
            });

            long count = TestContainerUtil.singleResultFirstColumn(session, "MATCH (f:Man) RETURN count(*) as c");
            assertEquals(1L, count);
        }
    }

    @Test
    public void testTxIdAfterAsyncNewProcedures() throws Exception {
        final String name = "testTxIdAfterAsync";
        try (final Session session = cluster.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install('neo4j', $name, 'UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, \"_executed\") as prop " +
                            "	WITH prop.node as n " +
                            "	CREATE (z:SON {father:id(n)}) " +
                            "	CREATE (n)-[:GENERATED]->(z)', " +
                            "{phase:'afterAsync'})",
                    Map.of("name", name));
        }
        try (final Session session = cluster.getSession()) {
            awaitProcedureInstalled(session, name);

            session.run("CREATE (:TEST {name:'x', _executed:0})");
            session.run("CREATE (:TEST {name:'y', _executed:0})");
            assertEventually(() -> TestContainerUtil.<Long>singleResultFirstColumn(session, "MATCH p = ()-[r:GENERATED]->() RETURN count(p) AS count"),
                    (value) -> value == 2L, 30, TimeUnit.SECONDS);
        }
    }

    private static void awaitProcedureInstalled(Session session, String name) {
        assertEventually(() -> session
                        .readTransaction(tx -> tx.run("CALL apoc.trigger.list")
                                .single().get("name").asString()),
                name::equals,
                30, TimeUnit.SECONDS);
    }

    @Test
    public void testTriggerCreatedInCorrectDatabase() {
        final String name = "testDatabase";
        try (final Session session = cluster.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            session.run("CALL apoc.trigger.install($dbName, $name, 'RETURN 1', " +
                            "{phase:'afterAsync'})",
                    Map.of("dbName", DB_FOO, "name", name));
        }
        try (final Session session = cluster.getDriver().session(forDatabase(DB_FOO))) {
            awaitProcedureInstalled(session, name);
        }
        try (final Session session = cluster.getDriver().session(forDatabase(DEFAULT_DATABASE_NAME))) {
            TestContainerUtil.testResult(session, "CALL apoc.trigger.list() " +
                            "YIELD name WHERE name = $name RETURN name",
                    Map.of("name", name),
                    res -> assertFalse(res.hasNext()));
        }
    }
}
