package apoc.trigger;

import apoc.create.Create;
import apoc.nodes.Nodes;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.trigger.TriggerTestUtil.TRIGGER_DEFAULT_REFRESH;
import static apoc.util.TestUtil.testCallCountEventually;
import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;

public class TriggerNewProceduresExtendedTest {
    private static final long TIMEOUT = 30L;

    private static final File directory = new File("target/conf");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // we cannot set via ApocConfig.apocConfig().setProperty("apoc.trigger.refresh", "100") in `setUp`, because is too late
        final File conf = new File(directory, "apoc.conf");
        try (FileWriter writer = new FileWriter(conf)) {
            writer.write(String.join("\n",
                    "apoc.trigger.refresh=" + TRIGGER_DEFAULT_REFRESH,
                    "apoc.trigger.enabled=true"));
        }
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + directory.getAbsolutePath());

        databaseManagementService = new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath())
                .setConfig(procedure_unrestricted, List.of("apoc*"))
                .build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        TestUtil.registerProcedure(sysDb, TriggerNewProcedures.class, Trigger.class, TriggerExtended.class,
                Nodes.class, Create.class);
        TestUtil.registerProcedure(db, Trigger.class);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        databaseManagementService.shutdown();
    }

    @After
    public void after() throws Exception {
        sysDb.executeTransactionally("CALL apoc.trigger.dropAll('neo4j')");
        testCallCountEventually(db, "CALL apoc.trigger.list", 0, TIMEOUT);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    private void awaitProcedureUpdated(String name, String query) {
        testCallEventually(db, "CALL apoc.trigger.list() YIELD name, query WHERE name = $name RETURN query",
                Map.of("name", name),
                row -> assertEquals(query, row.get("query")),
                TIMEOUT);
    }

    //
    // test cases taken and adapted from TriggerExtendedTest.java
    //

    @Test
    public void testTimeStampTriggerForUpdatedProperties() {
        final String name = "timestamp";
        final String query = "UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()";
        sysDb.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        db.executeTransactionally("CREATE (f:Foo) SET f.foo='bar'");
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f",
                (row) -> assertTrue(((Node) row.get("f")).hasProperty("ts")));
    }

    @Test
    public void testLowerCaseName() {
        db.executeTransactionally("create constraint for (p:Person) require p.id is unique");
        final String name = "lowercase";
        final String query = "UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n.id = toLower(n.name)";
        sysDb.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        db.executeTransactionally("CREATE (f:Person {name:'John Doe'})");
        TestUtil.testCall(db, "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node)row.get("f")).getProperty("id"));
            assertEquals("John Doe", ((Node)row.get("f")).getProperty("name"));
        });
    }

    @Test
    public void testSetLabels() {
        db.executeTransactionally("CREATE (f {name:'John Doe'})");
        final String name = "setlabels";
        final String query = "UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n:Man";
        sysDb.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        db.executeTransactionally("MATCH (f) SET f:Person");
        TestUtil.testCall(db, "MATCH (f:Man) RETURN f", (row) -> {
            assertEquals("John Doe", ((Node)row.get("f")).getProperty("name"));
            assertTrue(((Node) row.get("f")).hasLabel(Label.label("Person")));
        });

        long count = TestUtil.singleResultFirstColumn(db, "MATCH (f:Man) RETURN count(*) as c");
        assertEquals(1L, count);
    }

    @Test
    public void testTxIdAfterAsync() {
        final String name = "triggerTest";
        final String query = "UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, \"_executed\") as prop " +
                "	WITH prop.node as n " +
                "	CREATE (z:SON {father:id(n)}) " +
                "	CREATE (n)-[:GENERATED]->(z)";
        sysDb.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase:'afterAsync'})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        db.executeTransactionally("CREATE (:TEST {name:'x', _executed:0})");
        db.executeTransactionally("CREATE (:TEST {name:'y', _executed:0})");
        org.neo4j.test.assertion.Assert.assertEventually(() -> db.executeTransactionally("MATCH p = ()-[r:GENERATED]->() RETURN count(p) AS count",
                        Collections.emptyMap(), (r) -> r.<Long>columnAs("count").next()),
                (value) -> value == 2L, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testIssue1152Before() {
        testIssue1152Common("before");
    }

    @Test
    public void testIssue1152After() {
        testIssue1152Common("after");
    }

    private void testIssue1152Common(String phase) {
        db.executeTransactionally("CREATE (n:To:Delete {prop1: 'val1', prop2: 'val2'}) RETURN id(n) as id");

        // we check also that we can execute write operation (through virtualNode functions, e.g. apoc.create.addLabels)
        final String name = "issue1152";
        final String query = "UNWIND $deletedNodes as deletedNode " +
                "WITH apoc.trigger.toNode(deletedNode, $removedLabels, $removedNodeProperties) AS deletedNode " +
                "CREATE (r:Report {id: id(deletedNode)}) WITH r, deletedNode " +
                "CALL apoc.create.addLabels(r, apoc.node.labels(deletedNode)) yield node with node, deletedNode " +
                "set node+=apoc.any.properties(deletedNode)";

        sysDb.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query,{phase: $phase})",
                Map.of("name", name, "query", query, "phase", phase));
        awaitProcedureUpdated(name, query);

        db.executeTransactionally("MATCH (f:To:Delete) DELETE f");

        TestUtil.testCall(db, "MATCH (n:Report:To:Delete) RETURN n", (row) -> {
            final Node n = (Node) row.get("n");
            assertEquals("val1", n.getProperty("prop1"));
            assertEquals("val2", n.getProperty("prop2"));
        });
    }

    @Test
    public void testRetrievePropsDeletedRelationship() {
        db.executeTransactionally("CREATE (s:Start)-[r:MY_TYPE {prop1: 'val1', prop2: 'val2'}]->(e:End), (s)-[:REMAINING_REL]->(e)");

        final String query = "UNWIND $deletedRelationships as deletedRel " +
                "WITH apoc.trigger.toRelationship(deletedRel, $removedRelationshipProperties) AS deletedRel " +
                "MATCH (s)-[r:REMAINING_REL]->(e) WITH r, deletedRel " +
                "set r+=apoc.any.properties(deletedRel), r.type= type(deletedRel)";

        final String assertionQuery = "MATCH (:Start)-[n:REMAINING_REL]->(:End) RETURN n";
        testRetrievePropsDeletedRelationshipCommon("before", query, assertionQuery);
        testRetrievePropsDeletedRelationshipCommon("after", query, assertionQuery);
    }

    @Test
    public void testRetrievePropsDeletedRelationshipWithQueryCreation() {
        db.executeTransactionally("CREATE (:Start)-[r:MY_TYPE {prop1: 'val1', prop2: 'val2'}]->(:End)");

        final String query = "UNWIND $deletedRelationships as deletedRel " +
                "WITH apoc.trigger.toRelationship(deletedRel, $removedRelationshipProperties) AS deletedRel " +
                "CREATE (r:Report {type: type(deletedRel)}) WITH r, deletedRel " +
                "set r+=apoc.any.properties(deletedRel)";

        final String assertionQuery = "MATCH (n:Report) RETURN n";
        testRetrievePropsDeletedRelationshipCommon("before", query, assertionQuery);
        testRetrievePropsDeletedRelationshipCommon("after", query, assertionQuery);
    }

    private void testRetrievePropsDeletedRelationshipCommon(String phase, String triggerQuery, String assertionQuery) {
        final String name = UUID.randomUUID().toString();
        sysDb.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query,{phase: $phase})",
                Map.of("name", name, "query", triggerQuery, "phase", phase));
        awaitProcedureUpdated(name, triggerQuery);
        db.executeTransactionally("MATCH (:Start)-[r:MY_TYPE]->(:End) DELETE r");

        TestUtil.testCall(db, assertionQuery, (row) -> {
            final Entity n = (Entity) row.get("n");
            assertEquals("MY_TYPE", n.getProperty("type"));
            assertEquals("val1", n.getProperty("prop1"));
            assertEquals("val2", n.getProperty("prop2"));
        });
    }
}