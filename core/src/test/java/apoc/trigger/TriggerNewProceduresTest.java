/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.trigger;

import static apoc.trigger.TriggerNewProcedures.*;
import static apoc.trigger.TriggerTestUtil.TIMEOUT;
import static apoc.trigger.TriggerTestUtil.TRIGGER_DEFAULT_REFRESH;
import static apoc.trigger.TriggerTestUtil.awaitTriggerDiscovered;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.assertion.Assert.assertEventually;

import apoc.nodes.Nodes;
import apoc.schema.Schemas;
import apoc.util.TestUtil;
import apoc.util.Util;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class TriggerNewProceduresTest {
    private static final File directory = new File("target/conf");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    // we cannot set via ApocConfig.apocConfig().setProperty("apoc.trigger.refresh", "2000") in `setUp`, because is too
    // late
    @ClassRule
    public static final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(
                    "apoc.trigger.refresh", String.valueOf(TRIGGER_DEFAULT_REFRESH))
            .and("apoc.trigger.enabled", "true");

    @BeforeClass
    public static void beforeClass() {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(
                        storeDir.getRoot().toPath())
                .setConfig(procedure_unrestricted, List.of("apoc*"))
                .build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        TestUtil.registerProcedure(sysDb, TriggerNewProcedures.class, Nodes.class, Schemas.class);
        TestUtil.registerProcedure(db, Trigger.class, Nodes.class, Schemas.class);
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

    //
    // test cases taken and adapted from TriggerTest.java
    //

    @Test
    public void testListTriggers() {
        String name = "count-removals";
        String query = "MATCH (c:Counter) SET c.count = c.count + size([f IN $deletedNodes WHERE id(f) > 0])";
        testCallCount(
                sysDb, "CALL apoc.trigger.install('neo4j', $name, $query,{})", map("query", query, "name", name), 1);

        testCallEventually(
                db,
                "CALL apoc.trigger.list",
                row -> {
                    assertEquals("count-removals", row.get("name"));
                    assertEquals(query, row.get("query"));
                    assertEquals(true, row.get("installed"));
                },
                TIMEOUT);
    }

    @Test
    public void testRemoveNode() {
        db.executeTransactionally("CREATE (:Counter {count:0})");
        db.executeTransactionally("CREATE (f:Foo)");
        final String name = "count-removals";
        final String query = "MATCH (c:Counter) SET c.count = c.count + size($deletedNodes)";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (f:Foo) DELETE f");
        testCallEventually(
                db,
                "MATCH (c:Counter) RETURN c.count as count",
                (row) -> {
                    assertEquals(1L, row.get("count"));
                },
                TIMEOUT);
    }

    @Test
    public void testDropStopAndStartNotExistentTrigger() {
        testCallEmpty(
                sysDb,
                "CALL apoc.trigger.stop('neo4j', $name)",
                map("name", UUID.randomUUID().toString()));

        testCallEmpty(
                sysDb,
                "CALL apoc.trigger.start('neo4j', $name)",
                map("name", UUID.randomUUID().toString()));

        testCallEmpty(
                sysDb,
                "CALL apoc.trigger.drop('neo4j', $name)",
                map("name", UUID.randomUUID().toString()));
    }

    @Test
    public void testOverwriteTrigger() {
        final String name = UUID.randomUUID().toString();

        String queryOne = "RETURN 111";
        testCall(
                sysDb,
                "CALL apoc.trigger.install('neo4j', $name, $query, {})",
                map("name", name, "query", queryOne),
                r -> {
                    assertEquals(name, r.get("name"));
                    assertEquals(queryOne, r.get("query"));
                });

        String queryTwo = "RETURN 999";
        testCall(
                sysDb,
                "CALL apoc.trigger.install('neo4j', $name, $query, {})",
                map("name", name, "query", queryTwo),
                r -> {
                    assertEquals(name, r.get("name"));
                    assertEquals(queryTwo, r.get("query"));
                });
    }

    @Test
    public void testIssue2247() {
        db.executeTransactionally("CREATE (n:ToBeDeleted)");
        final String name = "myTrig";
        final String query = "RETURN 1";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (n:ToBeDeleted) DELETE n");
        sysDb.executeTransactionally("CALL apoc.trigger.drop('neo4j', 'myTrig')");
        testCallCountEventually(db, "CALL apoc.trigger.list", 0, TIMEOUT);
    }

    @Test
    public void testRemoveRelationship() throws Exception {
        db.executeTransactionally("CREATE (:Counter {count:0})");
        db.executeTransactionally("CREATE (f:Foo)-[:X]->(f)");

        String name = "count-removed-rels";
        String query = "MATCH (c:Counter) SET c.count = c.count + size($deletedRelationships)";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (f:Foo) DETACH DELETE f");
        testCall(db, "MATCH (c:Counter) RETURN c.count as count", (row) -> {
            assertEquals(1L, row.get("count"));
        });
    }

    @Test
    public void testRemoveTrigger() throws Exception {
        testCallCount(sysDb, "CALL apoc.trigger.install('neo4j', 'to-be-removed','RETURN 1',{})", 1);
        testCallEventually(
                db,
                "CALL apoc.trigger.list()",
                (row) -> {
                    assertEquals("to-be-removed", row.get("name"));
                    assertEquals("RETURN 1", row.get("query"));
                    assertEquals(true, row.get("installed"));
                },
                TIMEOUT);

        testCall(sysDb, "CALL apoc.trigger.drop('neo4j', 'to-be-removed')", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertEquals("RETURN 1", row.get("query"));
            assertEquals(false, row.get("installed"));
        });
        testCallCountEventually(db, "CALL apoc.trigger.list()", 0, TIMEOUT);

        testCallEmpty(sysDb, "CALL apoc.trigger.drop('neo4j', 'to-be-removed')", Collections.emptyMap());
    }

    @Test
    public void testRemoveAllTrigger() throws Exception {
        testCallCount(sysDb, "CALL apoc.trigger.dropAll('neo4j')", 0);
        testCallCountEventually(db, "call apoc.trigger.list", 0, TIMEOUT);

        testCallCount(sysDb, "CALL apoc.trigger.install('neo4j', 'to-be-removed-1','RETURN 1',{})", 1);
        testCallCount(sysDb, "CALL apoc.trigger.install('neo4j', 'to-be-removed-2','RETURN 2',{})", 1);

        testCallCountEventually(db, "call apoc.trigger.list", 2, TIMEOUT);

        TestUtil.testResult(sysDb, "CALL apoc.trigger.dropAll('neo4j')", (res) -> {
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

        testCallCount(sysDb, "CALL apoc.trigger.dropAll('neo4j')", 0);
    }

    @Test
    public void testTimeStampTrigger() throws Exception {
        String name = "timestamp";
        String query = "UNWIND $createdNodes AS n SET n.ts = timestamp()";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("CREATE (f:Foo)");
        testCall(db, "MATCH (f:Foo) RETURN f", (row) -> assertTrue(((Node) row.get("f")).hasProperty("ts")));
    }

    @Test
    public void testTxId() throws Exception {
        long start = System.currentTimeMillis();
        String name = "txinfo";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase:'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("CREATE (f:Bar)");
        testCall(db, "MATCH (f:Bar) RETURN f", (row) -> {
            assertTrue((Long) ((Node) row.get("f")).getProperty("txId") > -1L);
            assertTrue((Long) ((Node) row.get("f")).getProperty("txTime") > start);
        });
    }

    @Test
    public void testTxIdWithRelationshipsAndAfter() {
        db.executeTransactionally("CREATE (:RelationshipCounter {count:0})");
        String name = UUID.randomUUID().toString();

        // We need to filter $createdRelationships, i.e. `size(..) > 0`, not to cause a deadlock
        String query = "WITH size($createdRelationships) AS sizeRels \n" + "WHERE sizeRels > 0 \n"
                + "MATCH (c:RelationshipCounter) \n"
                + "SET c.txId = $transactionId  SET c.count = c.count + sizeRels";

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("CREATE (a:A)<-[r:C]-(b:B)");

        testCall(db, "MATCH (n:RelationshipCounter) RETURN n", this::testTxIdWithRelsAssertionsCommon);
    }

    @Test
    public void testTxIdWithRelationshipsAndAfterAsync() {
        testTxIdWithRelationshipsCommon("afterAsync");

        testCallEventually(db, "MATCH (n:RelationshipCounter) RETURN n", this::testTxIdWithRelsAssertionsCommon, 10);
    }

    private void testTxIdWithRelsAssertionsCommon(Map<String, Object> r) {
        final Node n = (Node) r.get("n");
        assertEquals(1L, n.getProperty("count"));
        final long txId = (long) n.getProperty("txId");
        assertTrue("Current txId is: " + txId, txId > -1L);
    }

    @Test
    public void testTxIdWithRelationshipsAndBefore() {
        testTxIdWithRelationshipsCommon("before");

        testCall(db, "MATCH (n:RelationshipCounter) RETURN n", r -> {
            final Node n = (Node) r.get("n");
            assertEquals(-1L, n.getProperty("txId"));
            assertEquals(1L, n.getProperty("count"));
        });
    }

    private static void testTxIdWithRelationshipsCommon(String phase) {
        String query = "MATCH (c:RelationshipCounter) \n" + "SET c.count = c.count + size($createdRelationships) \n"
                + "SET c.txId = $transactionId";

        db.executeTransactionally("CREATE (:RelationshipCounter {count:0})");
        String name = UUID.randomUUID().toString();

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install( 'neo4j', $name, $query, {phase: $phase})",
                Map.of("name", name, "query", query, "phase", phase));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("CREATE (a:A)<-[r:C]-(b:B)");
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
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase:$phase})",
                Map.of("name", name, "query", query, "phase", phase));
        awaitTriggerDiscovered(db, name, query);

        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((TransactionImpl) tx).kernelTransaction();
            ktx.setMetaData(Collections.singletonMap("txMeta", "hello"));
            tx.execute("CREATE (f:Bar)");
            tx.commit();
        }
        testCall(db, "MATCH (f:Bar) RETURN f", (row) -> {
            assertEquals("hello", ((Node) row.get("f")).getProperty("txMeta"));
        });
    }

    @Test
    public void testPauseResult() {
        String name = "pausedTest";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);
        testCall(sysDb, "CALL apoc.trigger.stop('neo4j', 'pausedTest')", (row) -> {
            assertEquals("pausedTest", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(true, row.get("paused"));
        });
    }

    @Test
    public void testPauseOnCallList() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        testCallEventually(
                db,
                "CALL apoc.trigger.list()",
                (row) -> {
                    assertEquals("test", row.get("name"));
                    assertEquals(true, row.get("installed"));
                    assertEquals(true, row.get("paused"));
                },
                TIMEOUT);
    }

    @Test
    public void testResumeResult() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        testCallEventually(db, "CALL apoc.trigger.list", row -> assertEquals(true, row.get("paused")), TIMEOUT);
        testCall(sysDb, "CALL apoc.trigger.start('neo4j', 'test')", (row) -> {
            assertEquals("test", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(false, row.get("paused"));
        });
        testCallEventually(db, "CALL apoc.trigger.list", row -> assertEquals(false, row.get("paused")), TIMEOUT);
    }

    @Test
    public void testTriggerPause() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        awaitTriggerDiscovered(db, name, query, true);

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
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        awaitTriggerDiscovered(db, name, query, true);

        sysDb.executeTransactionally("CALL apoc.trigger.start('neo4j', 'test')");
        awaitTriggerDiscovered(db, name, query, false);

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
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', 'test','UNWIND $createdNodes AS n SET n.txId = , n.txTime = $commitTime',{})");
    }

    @Test
    public void testCreatedRelationshipsAsync() {
        db.executeTransactionally("CREATE (:A {name: \"A\"})-[:R1]->(:Z {name: \"Z\"})");
        String name = "trigger-after-async";
        final String query = "UNWIND $createdRelationships AS r\n" + "MATCH (a:A)-[r]->(z:Z)\n"
                + "WHERE type(r) IN [\"R2\", \"R3\"]\n"
                + "MATCH (a)-[r1:R1]->(z)\n"
                + "SET r1.triggerAfterAsync = true";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (a:A {name: \"A\"})-[:R1]->(z:Z {name: \"Z\"})\n" + "MERGE (a)-[:R2]->(z)");

        assertEventually(
                () -> db.executeTransactionally("MATCH ()-[r:R1]->() RETURN r", Map.of(), result ->
                        (boolean) result.<Relationship>columnAs("r").next().getProperty("triggerAfterAsync", false)),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }

    @Test
    public void testDeleteRelationshipsAsync() {
        db.executeTransactionally(
                "CREATE (a:A {name: \"A\"})-[:R1 {omega: 3}]->(z:Z {name: \"Z\"}), (a)-[:R2 {alpha: 1}]->(z)");
        String name = "trigger-after-async-1";
        final String query = "UNWIND $deletedRelationships AS r\n" + "MATCH (a)-[r1:R1]->(z)\n"
                + "SET a.alpha = apoc.any.property(r, \"alpha\"), r1.triggerAfterAsync = size($deletedRelationships) > 0, r1.size = size($deletedRelationships), r1.deleted = type(r) RETURN *";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        // delete rel
        commonDeleteAfterAsync("MATCH (a:A {name: 'A'})-[r:R2]->(z:Z {name: 'Z'}) DELETE r");
    }

    @Test
    public void testDeleteRelationshipsAsyncWithCreationInQuery() {
        db.executeTransactionally(
                "CREATE (a:A {name: \"A\"})-[:R1 {omega: 3}]->(z:Z {name: \"Z\"}), (a)-[:R2 {alpha: 1}]->(z)");
        String name = "trigger-after-async-2";
        final String query = "UNWIND $deletedRelationships AS r\n" + "MATCH (a)-[r1:R1]->(z)\n"
                + "SET a.alpha = apoc.any.property(r, \"alpha\"), r1.triggerAfterAsync = size($deletedRelationships) > 0, r1.size = size($deletedRelationships), r1.deleted = type(r) RETURN *";
        db.executeTransactionally(
                "CALL apoc.trigger.add($name, $query, {phase: 'afterAsync'})", map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        // delete rel
        commonDeleteAfterAsync("MATCH (a:A {name: 'A'})-[r:R2]->(z:Z {name: 'Z'}) DELETE r");
    }

    @Test
    public void testDeleteNodesAsync() {
        db.executeTransactionally(
                "CREATE (a:A {name: 'A'})-[:R1 {omega: 3}]->(z:Z {name: 'Z'}), (:R2:Other {alpha: 1})");
        String name = "trigger-after-async-3";
        final String query = "UNWIND $deletedNodes AS n\n" + "MATCH (a)-[r1:R1]->(z)\n"
                + "SET a.alpha = apoc.any.property(n, \"alpha\"), r1.triggerAfterAsync = size($deletedNodes) > 0, r1.size = size($deletedNodes), r1.deleted = \"R2\" RETURN *";

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', 'trigger-after-async-3', $query, {phase: 'afterAsync'})",
                map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        // delete node
        commonDeleteAfterAsync("MATCH (n:R2) DELETE n");
    }

    @Test
    public void testDeleteNodesAsyncWithCreationQuery() {
        db.executeTransactionally("CREATE (:R2:Other {alpha: 1})");
        String name = "trigger-after-async-4";
        final String query = "UNWIND $deletedNodes AS n\n" + "CREATE (a:A)-[r1:R1 {omega: 3}]->(z:Z)\n"
                + "SET a.alpha = 1, r1.triggerAfterAsync = size($deletedNodes) > 0, r1.size = size($deletedNodes), r1.deleted = apoc.node.labels(n)[0] RETURN *";

        db.executeTransactionally(
                "CALL apoc.trigger.add($name, $query, {phase: 'afterAsync'})", map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        // delete node
        commonDeleteAfterAsync("MATCH (n:R2) DELETE n");
    }

    private void commonDeleteAfterAsync(String deleteQuery) {
        db.executeTransactionally(deleteQuery);

        final Map<String, Object> expectedProps =
                Map.of("deleted", "R2", "triggerAfterAsync", true, "size", 1L, "omega", 3L);

        assertEventually(
                () -> db.executeTransactionally("MATCH (a:A {alpha: 1})-[r:R1]->() RETURN r", Map.of(), result -> {
                    final ResourceIterator<Relationship> relIterator = result.columnAs("r");
                    return relIterator.hasNext();
                }),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }

    @Test
    public void testDeleteRelationships() {
        db.executeTransactionally("CREATE (a:A {name: \"A\"})-[:R1]->(z:Z {name: \"Z\"}), (a)-[:R2]->(z)");
        String name = "trigger-after-async-3";
        final String query = "UNWIND $deletedRelationships AS r\n" + "MERGE (a:AA{name: \"AA\"})\n"
                + "SET a.triggerAfter = size($deletedRelationships) = 1, a.deleted = type(r)";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (a:A {name: \"A\"})-[r:R2]->(z:Z {name: \"Z\"})\n" + "DELETE r");

        assertEventually(
                () -> db.executeTransactionally("MATCH (a:AA) RETURN a", Map.of(), result -> {
                    final Node r = result.<Node>columnAs("a").next();
                    return (boolean) r.getProperty("triggerAfter", false)
                            && r.getProperty("deleted", "").equals("R2");
                }),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }

    //
    // new test cases
    //

    @Test
    public void testTriggerShouldNotCauseCascadeTransactionWithPhaseBefore() {
        testCascadeTransactionCommon("before");
    }

    @Test
    public void testTriggerShouldNotCauseCascadeTransactionWithPhaseAfter() {
        testCascadeTransactionCommon("after");
    }

    @Test
    public void testTriggerShouldNotCauseCascadeTransactionWithPhaseAfterAsync() {
        testCascadeTransactionCommon("afterAsync");
    }

    private static void testCascadeTransactionCommon(String phase) {
        db.executeTransactionally("CREATE (:TransactionCounter {count:0});");

        // create the trigger
        String query = "MATCH (c:TransactionCounter) SET c.count = c.count + 1";
        String name = "count-relationships";

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: $phase})",
                Map.of("name", name, "query", query, "phase", phase));

        awaitTriggerDiscovered(db, name, query);

        String countQuery = "MATCH (n:TransactionCounter) RETURN n.count as count";
        long countBefore = singleResultFirstColumn(db, countQuery);
        assertEquals(0L, countBefore);

        // activate the trigger handler
        db.executeTransactionally("CREATE (a:A)");

        // assert that `count` eventually increment by 1
        assertEventually(
                () -> (long) singleResultFirstColumn(db, countQuery), (value) -> value == 1L, 10L, TimeUnit.SECONDS);

        // assert that `count` remains 1 and doesn't increment in subsequent transactions
        for (int i = 0; i < 10; i++) {
            long count = singleResultFirstColumn(db, countQuery);
            assertEquals(1L, count);
            Util.sleep(100);
        }
    }

    @Test
    public void testTriggerShow() {
        String name = "test-show1";
        String name2 = "test-show2";
        String query = "MATCH (c:TestShow) SET c.count = 1";

        testCall(
                sysDb,
                "CALL apoc.trigger.install('neo4j', $name, $query,{}) YIELD name",
                map("query", query, "name", name),
                r -> assertEquals(name, r.get("name")));

        testCall(
                sysDb,
                "CALL apoc.trigger.install('neo4j', $name, $query,{}) YIELD name",
                map("query", query, "name", name2),
                r -> assertEquals(name2, r.get("name")));

        // not updated
        testResult(
                sysDb,
                "CALL apoc.trigger.show('neo4j') " + "YIELD name, query RETURN * ORDER BY name",
                map("query", query, "name", name),
                res -> {
                    Map<String, Object> row = res.next();
                    assertEquals(name, row.get("name"));
                    assertEquals(query, row.get("query"));
                    row = res.next();
                    assertEquals(name2, row.get("name"));
                    assertEquals(query, row.get("query"));
                    assertFalse(res.hasNext());
                });
    }

    @Test
    public void testInstallTriggerInUserDb() {
        try {
            testCall(
                    db,
                    "CALL apoc.trigger.install('neo4j', 'userDb', 'RETURN 1',{})",
                    r -> fail("Should fail because of user db execution"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(TRIGGER_NOT_ROUTED_ERROR));
        }
    }

    // TODO - it should be removed/ignored in 5.x, due to Util.validateQuery(..) removal
    @Test
    public void testInstallTriggerInWrongDb() {
        try {
            testCall(
                    sysDb,
                    "CALL apoc.trigger.install('notExistent', 'name', 'RETURN 1',{})",
                    r -> fail("Should fail because of database not found"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(DatabaseNotFoundException.class.getName()));
        }
    }

    @Test
    public void testShowTriggerInUserDb() {
        try {
            testCall(db, "CALL apoc.trigger.show('neo4j')", r -> fail("Should fail because of user db execution"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(NON_SYS_DB_ERROR));
        }
    }

    @Test
    public void testInstallTriggerInSystemDb() {
        try {
            testCall(sysDb, "CALL apoc.trigger.install('system', 'name', 'SHOW DATABASES', {})", r -> fail(""));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(TRIGGER_BAD_TARGET_ERROR));
        }
    }

    @Test
    public void testEventualConsistency() {
        long count = 0L;
        final String name = UUID.randomUUID().toString();
        final String query = "UNWIND $createdNodes AS n SET n.count = " + count;

        // this does nothing, just to test consistency with multiple triggers
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, 'return 1', {phase: 'after'})",
                map("name", UUID.randomUUID().toString()));

        // create trigger
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);
        db.executeTransactionally("CREATE (n:Something)");

        // check trigger
        testCall(db, "MATCH (c:Something) RETURN c.count as count", (row) -> assertEquals(count, row.get("count")));

        // stop trigger
        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', $name)", map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query, true);

        // this does nothing, just to test consistency with multiple triggers
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, 'return 1', {phase: 'after'})",
                map("name", UUID.randomUUID().toString()));
    }

    @Test
    public void testTriggerInstallWithSchemaCommand() {
        String query = "CREATE INDEX periodicIdx FOR (n:Bar) ON (n.first_name, n.last_name)";
        try {
            testCall(
                    sysDb,
                    "CALL apoc.trigger.install('neo4j', 'triggerSchema', 'CREATE INDEX periodicIdx FOR (n:Bar) ON (n.first_name, n.last_name)', {phase: 'before'})",
                    Map.of("query", query),
                    (row) -> fail("Should fail because of unsupported schema command"));
        } catch (RuntimeException e) {
            final String expected = "Failed to invoke procedure `apoc.trigger.install`: "
                    + "Caused by: java.lang.RuntimeException: " + TRIGGER_QUERY_TYPES_ERROR;
            assertEquals(expected, e.getMessage());
        }
    }

    @Test
    public void testTriggerInstallWithSchemaProcedure() {
        String query = "CREATE INDEX periodicIdx FOR (n:Bar) ON (n.first_name, n.last_name)";
        try {
            testCall(
                    sysDb,
                    "CALL apoc.trigger.install('neo4j', 'triggerSchemaProc', 'CALL apoc.schema.assert({}, {})', {phase: 'before'})",
                    Map.of("query", query),
                    (row) -> fail("Should fail because of unsupported schema procedure"));
        } catch (RuntimeException e) {
            final String expected = "Failed to invoke procedure `apoc.trigger.install`: "
                    + "Caused by: java.lang.RuntimeException: " + TRIGGER_MODES_ERROR;
            assertEquals(expected, e.getMessage());
        }
    }
}
