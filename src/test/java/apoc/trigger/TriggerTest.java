package apoc.trigger;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 20.09.16
 */
public class TriggerTest {
    private GraphDatabaseService db;
    private long start;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("apoc.trigger.enabled","true")
                .newGraphDatabase();
        start = System.currentTimeMillis();
        TestUtil.registerProcedure(db, Trigger.class);
    }

    @After
    public void tearDown() {
        if (db!=null) db.shutdown();
    }

    @Test
    public void testListTriggers() throws Exception {
        String query = "MATCH (c:Counter) SET c.count = c.count + size([f IN {deletedNodes} WHERE id(f) > 0])";
        assertEquals(1, Iterators.count(db.execute("CALL apoc.trigger.add('count-removals',{query},{}) YIELD name RETURN name", map("query", query))));
        TestUtil.testCall(db, "CALL apoc.trigger.list()", (row) -> {
            assertEquals("count-removals", row.get("name"));
            assertEquals(query, row.get("query"));
            assertEquals(true, row.get("installed"));
        });
    }
    @Test
    public void testRemoveNode() throws Exception {
        db.execute("CREATE (:Counter {count:0})").close();
        db.execute("CREATE (f:Foo)").close();
        db.execute("CALL apoc.trigger.add('count-removals','MATCH (c:Counter) SET c.count = c.count + size([f IN {deletedNodes} WHERE id(f) > 0])',{})").close();
        db.execute("MATCH (f:Foo) DELETE f").close();
        TestUtil.testCall(db, "MATCH (c:Counter) RETURN c.count as count", (row) -> {
            assertEquals(1L, row.get("count"));
        });
    }

    @Test
    public void testRemoveRelationship() throws Exception {
        db.execute("CREATE (:Counter {count:0})").close();
        db.execute("CREATE (f:Foo)-[:X]->(f)").close();
        db.execute("CALL apoc.trigger.add('count-removed-rels','MATCH (c:Counter) SET c.count = c.count + size({deletedRelationships})',{})").close();
        db.execute("MATCH (f:Foo) DETACH DELETE f").close();
        TestUtil.testCall(db, "MATCH (c:Counter) RETURN c.count as count", (row) -> {
            assertEquals(1L, row.get("count"));
        });
    }

    @Test
    public void testRemoveTrigger() throws Exception {
        assertEquals(1, Iterators.count(db.execute("CALL apoc.trigger.add('to-be-removed','RETURN 1',{}) YIELD name RETURN name")));
        TestUtil.testCall(db, "CALL apoc.trigger.list()", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertEquals("RETURN 1", row.get("query"));
            assertEquals(true, row.get("installed"));
        });
        TestUtil.testCall(db, "CALL apoc.trigger.remove('to-be-removed')", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertEquals("RETURN 1", row.get("query"));
            assertEquals(false, row.get("installed"));
        });
        assertEquals(0, Iterators.count(db.execute("CALL apoc.trigger.list()")));
        TestUtil.testCall(db, "CALL apoc.trigger.remove('to-be-removed')", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertEquals(null, row.get("query"));
            assertEquals(false, row.get("installed"));
        });
    }
    @Test
    public void testRemoveAllTrigger() throws Exception {
        TestUtil.testCall(db, "CALL apoc.trigger.removeAll()", (row) -> {
            assertEquals(null, row.get("name"));
            assertEquals(null, row.get("query"));
            assertEquals(false, row.get("installed"));
        });
        assertEquals(1, Iterators.count(db.execute("CALL apoc.trigger.add('to-be-removed-1','RETURN 1',{}) YIELD name RETURN name")));
        assertEquals(1, Iterators.count(db.execute("CALL apoc.trigger.add('to-be-removed-2','RETURN 2',{}) YIELD name RETURN name")));
        assertEquals(2, Iterators.count(db.execute("CALL apoc.trigger.list()")));
        TestUtil.testResult(db, "CALL apoc.trigger.removeAll()", (res) -> {
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
        assertEquals(0, Iterators.count(db.execute("CALL apoc.trigger.list()")));
        TestUtil.testCall(db, "CALL apoc.trigger.removeAll()", (row) -> {
            assertEquals(null, row.get("name"));
            assertEquals(null, row.get("query"));
            assertEquals(false, row.get("installed"));
        });
    }

    @Test
    public void testTimeStampTrigger() throws Exception {
        db.execute("CALL apoc.trigger.add('timestamp','UNWIND {createdNodes} AS n SET n.ts = timestamp()',{})").close();
        db.execute("CREATE (f:Foo)").close();
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node)row.get("f")).hasProperty("ts"));
        });
    }

    @Test
    public void testTimeStampTriggerForUpdatedProperties() throws Exception {
        db.execute("CALL apoc.trigger.add('timestamp','UNWIND apoc.trigger.nodesByLabel({assignedNodeProperties},null) AS n SET n.ts = timestamp()',{})").close();
        db.execute("CREATE (f:Foo) SET f.foo='bar'").close();
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node)row.get("f")).hasProperty("ts"));
        });
    }

    @Test
    public void testLowerCaseName() throws Exception {
        db.execute("create constraint on (p:Person) assert p.id is unique").close();
        Trigger.TriggerHandler.getInstance().add("timestamp","UNWIND apoc.trigger.nodesByLabel({assignedLabels},'Person') AS n SET n.id = toLower(n.name)",null);
//        Trigger.TriggerHandler.add("lowercase","UNWIND {createdNodes} AS n SET n.id = toLower(n.name)",null);
        db.execute("CREATE (f:Person {name:'John Doe'})").close();
        TestUtil.testCall(db, "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node)row.get("f")).getProperty("id"));
            assertEquals("John Doe", ((Node)row.get("f")).getProperty("name"));
        });
    }
    @Test
    public void testSetLabels() throws Exception {
        db.execute("CREATE (f {name:'John Doe'})").close();
        Trigger.TriggerHandler.getInstance().add("timestamp","UNWIND apoc.trigger.nodesByLabel({assignedLabels},'Person') AS n SET n:Man",null);
        db.execute("MATCH (f) SET f:Person").close();
        TestUtil.testCall(db, "MATCH (f:Man) RETURN f", (row) -> {
            assertEquals("John Doe", ((Node)row.get("f")).getProperty("name"));
            assertEquals(true, ((Node)row.get("f")).hasLabel(Label.label("Person")));
        });
        ResourceIterator<Long> it = db.execute("MATCH (f:Man) RETURN count(*) as c").columnAs("c");
        assertEquals(1L,(long)it.next());
        it.close();
    }
    @Test
    public void testTxId() throws Exception {
        Trigger.TriggerHandler.getInstance().add("txInfo","UNWIND {createdNodes} AS n SET n.txId = {transactionId}, n.txTime = {commitTime}", map("phase","after"));
        db.execute("CREATE (f:Bar)").close();
        TestUtil.testCall(db, "MATCH (f:Bar) RETURN f", (row) -> {
            assertEquals(true, (Long)((Node)row.get("f")).getProperty("txId") > -1L);
            assertEquals(true, (Long)((Node)row.get("f")).getProperty("txTime") > start);
        });
    }
    
    @Test
    public void testPauseResult() throws Exception {
        Trigger.TriggerHandler.getInstance().add("test","UNWIND {createdNodes} AS n SET n.txId = {transactionId}, n.txTime = {commitTime}", map("phase","after") );
        TestUtil.testCall(db, "CALL apoc.trigger.pause('test')", (row) -> {
            assertEquals("test", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(true, row.get("paused"));
        });
    }

    @Test
    public void testPauseOnCallList() throws Exception {
        Trigger.TriggerHandler.getInstance().add("test","UNWIND {createdNodes} AS n SET n.txId = {transactionId}, n.txTime = {commitTime}", map("phase","after") );
        db.execute("CALL apoc.trigger.pause('test')");
        TestUtil.testCall(db, "CALL apoc.trigger.list()", (row) -> {
            assertEquals("test", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(true, row.get("paused"));
        });
    }

    @Test
    public void testResumeResult() throws Exception {
        Trigger.TriggerHandler.getInstance().add("test","UNWIND {createdNodes} AS n SET n.txId = {transactionId}, n.txTime = {commitTime}", map("phase","after") );
        db.execute("CALL apoc.trigger.pause('test')");
        TestUtil.testCall(db, "CALL apoc.trigger.resume('test')", (row) -> {
            assertEquals("test", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(false, row.get("paused"));
        });
    }

    @Test
    public void testTriggerPause() throws Exception {
        db.execute("CALL apoc.trigger.add('test','UNWIND {createdNodes} AS n SET n.txId = {transactionId}, n.txTime = {commitTime}',{})").close();
        db.execute("CALL apoc.trigger.pause('test')").close();
        db.execute("CREATE (f:Foo {name:'Michael'})").close();
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(false, ((Node)row.get("f")).hasProperty("txId"));
            assertEquals(false, ((Node)row.get("f")).hasProperty("txTime"));
            assertEquals(true, ((Node)row.get("f")).hasProperty("name"));
        });
    }

    @Test
    public void testTriggerResume() throws Exception {
        db.execute("CALL apoc.trigger.add('test','UNWIND {createdNodes} AS n SET n.txId = {transactionId}, n.txTime = {commitTime}',{})").close();
        db.execute("CALL apoc.trigger.pause('test')").close();
        db.execute("CALL apoc.trigger.resume('test')").close();
        db.execute("CREATE (f:Foo {name:'Michael'})").close();
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node)row.get("f")).hasProperty("txId"));
            assertEquals(true, ((Node)row.get("f")).hasProperty("txTime"));
            assertEquals(true, ((Node)row.get("f")).hasProperty("name"));
        });
    }



}
