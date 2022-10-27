package apoc.trigger;

import apoc.nodes.Nodes;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.ApocSettings.apoc_trigger_enabled;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testCallCountEventually;
import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.assertion.Assert.assertEventually;

/**
 * Test class for non-deprecated procedures, 
 * i.e. `apoc.trigger.install`, `apoc.trigger.drop`, `apoc.trigger.dropAll`, `apoc.trigger.stop`, and `apoc.trigger.start`
 */
public class TriggerNewProceduresTest {
    private static final long TIMEOUT = 1L;

    private static final File directory = new File("target/conf");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        // we cannot set via ApocConfig.apocConfig().setProperty("apoc.trigger.refresh", "100") in `setUp`, because is too late
        final File conf = new File(directory, "apoc.conf");
        try (FileWriter writer = new FileWriter(conf)) {
            writer.write("apoc.trigger.refresh=100");
        }
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + directory.getAbsolutePath());
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(procedure_unrestricted, List.of("apoc*"))
            .withSetting(apoc_trigger_enabled, true);  // need to use settings here, apocConfig().setProperty in `setUp` is too late

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Trigger.class, TriggerDeprecatedProcedures.class, Nodes.class);
    }

    private void awaitProcedureUpdated(String name, String query) {
        awaitProcedureUpdated(name, query, false);
    }
    
    private void awaitProcedureUpdated(String name, String query, boolean paused) {
        testCallEventually(db, "CALL apoc.trigger.list() YIELD name, query, paused WHERE name = $name RETURN query, paused",
                Map.of("name", name),
                row -> {
                    assertEquals(query, row.get("query"));
                    assertEquals(paused, row.get("paused"));
                }, TIMEOUT);
    }

    //
    // test cases taken and adapted from TriggerTest.java
    //
    
    @Test
    public void testListTriggers() {
        String name = "count-removals";
        String query = "MATCH (c:Counter) SET c.count = c.count + size([f IN $deletedNodes WHERE id(f) > 0])";
        testCallCount(db, "CALL apoc.trigger.install('neo4j', $name, $query,{}) YIELD name RETURN name",
                map("query", query, "name", name),
                1);

        testCallEventually(db, "CALL apoc.trigger.list", row -> {
            assertEquals("count-removals", row.get("name"));
            assertEquals(query, row.get("query"));
            assertEquals(true, row.get("installed"));
        }, TIMEOUT);

    }

    @Test
    public void testRemoveNode() {
        db.executeTransactionally("CREATE (:Counter {count:0})");
        db.executeTransactionally("CREATE (f:Foo)");
        final String name = "count-removals";
        final String query = "MATCH (c:Counter) SET c.count = c.count + size([f IN $deletedNodes WHERE id(f) > 0])";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);

        db.executeTransactionally("MATCH (f:Foo) DELETE f");
        testCallEventually(db, "MATCH (c:Counter) RETURN c.count as count", (row) -> {
            assertEquals(1L, row.get("count"));
        }, TIMEOUT);
    }

    @Test
    public void testIssue2247() {
        db.executeTransactionally("CREATE (n:ToBeDeleted)");
        final String name = "myTrig";
        final String query = "RETURN 1";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);

        db.executeTransactionally("MATCH (n:ToBeDeleted) DELETE n");
        db.executeTransactionally("CALL apoc.trigger.drop('neo4j', 'myTrig')");
        testCallCountEventually(db, "CALL apoc.trigger.list", 0, TIMEOUT);
    }

    @Test
    public void testRemoveRelationship() throws Exception {
        db.executeTransactionally("CREATE (:Counter {count:0})");
        db.executeTransactionally("CREATE (f:Foo)-[:X]->(f)");
        
        String name = "count-removed-rels";
        String query = "MATCH (c:Counter) SET c.count = c.count + size($deletedRelationships)";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);

        db.executeTransactionally("MATCH (f:Foo) DETACH DELETE f");
        testCall(db, "MATCH (c:Counter) RETURN c.count as count", (row) -> {
            assertEquals(1L, row.get("count"));
        });
    }

    @Test
    public void testRemoveTrigger() throws Exception {
        testCallCount(db, "CALL apoc.trigger.install('neo4j', 'to-be-removed','RETURN 1',{}) YIELD name RETURN name", 1);
        testCallEventually(db, "CALL apoc.trigger.list()", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertEquals("RETURN 1", row.get("query"));
            assertEquals(true, row.get("installed"));
        }, TIMEOUT);
        
        testCall(db, "CALL apoc.trigger.drop('neo4j', 'to-be-removed')", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertEquals("RETURN 1", row.get("query"));
            assertEquals(false, row.get("installed"));
        });
        testCallCountEventually(db, "CALL apoc.trigger.list()", 0, TIMEOUT);

        testCall(db, "CALL apoc.trigger.drop('neo4j', 'to-be-removed')", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertNull(row.get("query"));
            assertEquals(false, row.get("installed"));
        });
    }

    @Test
    public void testRemoveAllTrigger() throws Exception {
        testCallCount(db, "CALL apoc.trigger.dropAll('neo4j')", 0);
        testCallCountEventually(db, "call apoc.trigger.list", 0, TIMEOUT);

        testCallCount(db, "CALL apoc.trigger.install('neo4j', 'to-be-removed-1','RETURN 1',{}) YIELD name RETURN name", 1);
        testCallCount(db, "CALL apoc.trigger.install('neo4j', 'to-be-removed-2','RETURN 2',{}) YIELD name RETURN name", 1);

        testCallCountEventually(db, "call apoc.trigger.list", 2, TIMEOUT);
        
        TestUtil.testResult(db, "CALL apoc.trigger.dropAll('neo4j')", (res) -> {
            Map<String, Object> row = res.next();
            assertEquals("to-be-removed-1", row.get("name"));
            assertEquals("RETURN 1", row.get("query"));
            assertEquals(false, row.get("installed"));
            row = res.next();
            assertEquals("to-be-removed-2", row.get("name"));
            assertEquals("RETURN 2", row.get("query"));
            assertEquals(false, row.get("installed"));
            assertFalse(res.hasNext());
        });
        testCallCountEventually(db, "call apoc.trigger.list", 0, TIMEOUT);

        testCallCount(db, "CALL apoc.trigger.dropAll('neo4j')", 0);
    }

    @Test
    public void testTimeStampTrigger() throws Exception {
        String name = "timestamp";
        String query = "UNWIND $createdNodes AS n SET n.ts = timestamp()";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {})", 
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        
        db.executeTransactionally("CREATE (f:Foo)");
        testCall(db, "MATCH (f:Foo) RETURN f", 
                (row) -> assertTrue(((Node) row.get("f")).hasProperty("ts")));
    }

    @Test
    public void testTxId() throws Exception {
        long start = System.currentTimeMillis();
        String name = "txinfo";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase:'after'})", 
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        
        db.executeTransactionally("CREATE (f:Bar)");
        testCall(db, "MATCH (f:Bar) RETURN f", (row) -> {
            assertTrue((Long) ((Node) row.get("f")).getProperty("txId") > -1L);
            assertTrue((Long) ((Node) row.get("f")).getProperty("txTime") > start);
        });
    }

    @Test
    public void testMetaDataBefore() {
        testMetaData("before");
    }

    @Test
    public void testMetaDataAfter() {
        testMetaData("after");
    }

    private void testMetaData(String phase) {
        String name = "txinfo";
        String query = "UNWIND $createdNodes AS n SET n += $metaData";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase:$phase})",
                Map.of("name", name, "query", query, "phase", phase));
        awaitProcedureUpdated(name, query);

        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((TransactionImpl)tx).kernelTransaction();
            ktx.setMetaData(Collections.singletonMap("txMeta", "hello"));
            tx.execute("CREATE (f:Bar)");
            tx.commit();
        }
        testCall(db, "MATCH (f:Bar) RETURN f", (row) -> {
            assertEquals("hello",  ((Node) row.get("f")).getProperty("txMeta") );
        });
    }

    @Test
    public void testPauseResult() {
        String name = "pausedTest";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})", 
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        testCall(db, "CALL apoc.trigger.stop('neo4j', 'pausedTest')", (row) -> {
            assertEquals("pausedTest", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(true, row.get("paused"));
        });
    }

    @Test
    public void testPauseOnCallList() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})", 
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        
        db.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        testCallEventually(db, "CALL apoc.trigger.list()", (row) -> {
            assertEquals("test", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(true, row.get("paused"));
        }, TIMEOUT);
    }

    @Test
    public void testResumeResult() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        
        db.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        testCallEventually(db, "CALL apoc.trigger.list", 
                row -> assertEquals(true, row.get("paused")), 
                TIMEOUT);
        testCall(db, "CALL apoc.trigger.start('neo4j', 'test')", (row) -> {
            assertEquals("test", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(false, row.get("paused"));
        });
        testCallEventually(db, "CALL apoc.trigger.list",
                row -> assertEquals(false, row.get("paused")),
                TIMEOUT);
    }

    @Test
    public void testTriggerPause() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        
        db.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        awaitProcedureUpdated(name, query, true);

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})");
        testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertFalse(((Node) row.get("f")).hasProperty("txId"));
            assertFalse(((Node) row.get("f")).hasProperty("txTime"));
            assertTrue(((Node) row.get("f")).hasProperty("name"));
        });
    }

    @Test
    public void testTriggerResume() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);
        
        db.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        awaitProcedureUpdated(name, query, true);

        db.executeTransactionally("CALL apoc.trigger.start('neo4j', 'test')");
        awaitProcedureUpdated(name, query, false);

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})");
        testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertTrue(((Node) row.get("f")).hasProperty("txId"));
            assertTrue(((Node) row.get("f")).hasProperty("txTime"));
            assertTrue(((Node) row.get("f")).hasProperty("name"));
        });
    }

    // TODO - it should be removed/ignored in 5.x, due to Util.validateQuery(..) removal
    @Test(expected = QueryExecutionException.class)
    public void showThrowAnException() {
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', 'test','UNWIND $createdNodes AS n SET n.txId = , n.txTime = $commitTime',{})");
    }

    @Test
    public void testCreatedRelationshipsAsync() {
        db.executeTransactionally("CREATE (:A {name: \"A\"})-[:R1]->(:Z {name: \"Z\"})");
        String name = "trigger-after-async";
        final String query = "UNWIND $createdRelationships AS r\n" +
                "MATCH (a:A)-[r]->(z:Z)\n" +
                "WHERE type(r) IN [\"R2\", \"R3\"]\n" +
                "MATCH (a)-[r1:R1]->(z)\n" +
                "SET r1.triggerAfterAsync = true";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);

        db.executeTransactionally("MATCH (a:A {name: \"A\"})-[:R1]->(z:Z {name: \"Z\"})\n" +
                "MERGE (a)-[:R2]->(z)");

        assertEventually(() ->
                        db.executeTransactionally("MATCH ()-[r:R1]->() RETURN r", Map.of(),
                                result -> (boolean) result.<Relationship>columnAs("r").next()
                                        .getProperty("triggerAfterAsync", false))
                , (value) -> value, 30L, TimeUnit.SECONDS);
    }

    @Test
    public void testDeleteRelationshipsAsync() {
        db.executeTransactionally("CREATE (a:A {name: \"A\"})-[:R1 {omega: 3}]->(z:Z {name: \"Z\"}), (a)-[:R2 {alpha: 1}]->(z)");
        String name = "trigger-after-async-1";
        final String query = "UNWIND $deletedRelationships AS r\n" +
                "MATCH (a)-[r1:R1]->(z)\n" +
                "SET a.alpha = apoc.any.property(r, \"alpha\"), r1.triggerAfterAsync = size($deletedRelationships) > 0, r1.size = size($deletedRelationships), r1.deleted = type(r) RETURN *";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                map("name", name, "query", query));
        awaitProcedureUpdated(name, query);

        // delete rel
        commonDeleteAfterAsync("MATCH (a:A {name: 'A'})-[r:R2]->(z:Z {name: 'Z'}) DELETE r");
    }

    @Test
    public void testDeleteRelationshipsAsyncWithCreationInQuery() {
        db.executeTransactionally("CREATE (a:A {name: \"A\"})-[:R1 {omega: 3}]->(z:Z {name: \"Z\"}), (a)-[:R2 {alpha: 1}]->(z)");
        String name = "trigger-after-async-2";
        final String query = "UNWIND $deletedRelationships AS r\n" +
                "CREATE (a:A)-[r1:R1 {omega: 3}]->(z)\n" +
                "SET a.alpha = apoc.any.property(r, \"alpha\"), r1.triggerAfterAsync = size($deletedRelationships) > 0, r1.size = size($deletedRelationships), r1.deleted = type(r) RETURN *";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                map("name", name, "query", query));
        awaitProcedureUpdated(name, query);

        // delete rel
        commonDeleteAfterAsync("MATCH (a:A {name: 'A'})-[r:R2]->(z:Z {name: 'Z'}) DELETE r");
    }

    @Test
    public void testDeleteNodesAsync() {
        db.executeTransactionally("CREATE (a:A {name: 'A'})-[:R1 {omega: 3}]->(z:Z {name: 'Z'}), (:R2:Other {alpha: 1})");
        String name = "trigger-after-async-3";
        final String query = "UNWIND $deletedNodes AS n\n" +
                "MATCH (a)-[r1:R1]->(z)\n" +
                "SET a.alpha = apoc.any.property(n, \"alpha\"), r1.triggerAfterAsync = size($deletedNodes) > 0, r1.size = size($deletedNodes), r1.deleted = apoc.node.labels(n)[0] RETURN *";

        db.executeTransactionally("CALL apoc.trigger.install('neo4j', 'trigger-after-async-3', $query, {phase: 'afterAsync'})",
                map("name", name, "query", query));
        awaitProcedureUpdated(name, query);

        // delete node
        commonDeleteAfterAsync("MATCH (n:R2) DELETE n");
    }

    @Test
    public void testDeleteNodesAsyncWithCreationQuery() {
        db.executeTransactionally("CREATE (:R2:Other {alpha: 1})");
        String name = "trigger-after-async-3";
        final String query = "UNWIND $deletedNodes AS n\n" +
                "CREATE (a:A)-[r1:R1 {omega: 3}]->(z:Z)\n" +
                "SET a.alpha = apoc.any.property(n, \"alpha\"), r1.triggerAfterAsync = size($deletedNodes) > 0, r1.size = size($deletedNodes), r1.deleted = apoc.node.labels(n)[0] RETURN *";

        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                map("name", name, "query", query));
        awaitProcedureUpdated(name, query);

        // delete node
        commonDeleteAfterAsync("MATCH (n:R2) DELETE n");
    }

    private void commonDeleteAfterAsync(String deleteQuery) {
        db.executeTransactionally(deleteQuery);

        final Map<String, Object> expectedProps = Map.of("deleted", "R2",
                "triggerAfterAsync", true,
                "size", 1L,
                "omega", 3L);

        assertEventually(() ->
                        db.executeTransactionally("MATCH (a:A {alpha: 1})-[r:R1]->() RETURN r", Map.of(),
                                result -> {
                                    final ResourceIterator<Relationship> relIterator = result.columnAs("r");
                                    return relIterator.hasNext()
                                            && relIterator.next().getAllProperties().equals(expectedProps);
                                })
                , (value) -> value, 30L, TimeUnit.SECONDS);
    }

    @Test
    public void testDeleteRelationships() {
        db.executeTransactionally("CREATE (a:A {name: \"A\"})-[:R1]->(z:Z {name: \"Z\"}), (a)-[:R2]->(z)");
        String name = "trigger-after-async-3";
        final String query = "UNWIND $deletedRelationships AS r\n" +
                "MERGE (a:AA{name: \"AA\"})\n" +
                "SET a.triggerAfter = size($deletedRelationships) = 1, a.deleted = type(r)";
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitProcedureUpdated(name, query);


        db.executeTransactionally("MATCH (a:A {name: \"A\"})-[r:R2]->(z:Z {name: \"Z\"})\n" +
                "DELETE r");

        assertEventually(() ->
                        db.executeTransactionally("MATCH (a:AA) RETURN a", Map.of(),
                                result -> {
                                    final Node r = result.<Node>columnAs("a").next();
                                    return (boolean) r.getProperty("triggerAfter", false)
                                            && r.getProperty("deleted", "").equals("R2");
                                })
                , (value) -> value, 30L, TimeUnit.SECONDS);
    }

    
    //
    // new test cases
    //
    
    // TODO - it should be removed/ignored in 5.x, due to Util.validateQuery(..) removal
    @Test
    public void testInstallTriggerInWrongDb() {
        try {
            testCall(db, "CALL apoc.trigger.install('notExistent', 'name', 'RETURN 1',{})", 
                    r -> fail("Should fail because of database not found"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(DatabaseNotFoundException.class.getName()));
        }
    }

    // TODO - it should be removed/ignored in 5.x, due to Util.validateQuery(..) removal
    @Test
    public void testInstallTriggerInSystemDb() {
        try {
            testCall(db, "CALL apoc.trigger.install('system', 'name', 'RETURN 1',{})", 
                    r -> fail("Should fail because of unrecognised system procedure"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains("Not a recognised system command or procedure"));
        }
    }

}
