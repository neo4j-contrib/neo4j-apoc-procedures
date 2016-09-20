package apoc.trigger;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
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
    public void testRemoveNode() throws Exception {
        db.execute("CREATE (:Counter {count:0})").close();
        db.execute("CREATE (f:Foo)").close();
        db.execute("CALL apoc.trigger.add('count-removals','MATCH (c:Counter) SET c.count = c.count + size([f IN {deletedNodes} WHERE id(f) > 0])')").close();
        db.execute("MATCH (f:Foo) DELETE f").close();
        TestUtil.testCall(db, "MATCH (c:Count) RETURN c.count as count", (row) -> {
            assertEquals(1L, row.get("count"));
        });
    }
    @Test
    public void testRemoveRelationship() throws Exception {
        db.execute("CREATE (:Counter {count:0})").close();
        db.execute("CREATE (f:Foo)-[:X]->(f)").close();
        db.execute("CALL apoc.trigger.add('count-removed-rels','MATCH (c:Counter) SET c.count = c.count + size([r IN {deletedRelationships} WHERE type(r) = \"X\"])')").close();
        db.execute("MATCH (f:Foo) DETACH DELETE f").close();
        TestUtil.testCall(db, "MATCH (c:Count) RETURN c.count as count", (row) -> {
            assertEquals(1L, row.get("count"));
        });
    }

    @Test
    public void testTimeStampTrigger() throws Exception {
        db.execute("CALL apoc.trigger.add('timestamp','UNWIND {createdNodes} AS n SET n.ts = timestamp()')").close();
        db.execute("CREATE (f:Foo)").close();
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node)row.get("f")).hasProperty("ts"));
        });
    }

    @Test
    public void testLowerCaseName() throws Exception {
        db.execute("create constraint on (p:Person) assert p.id is unique").close();
//        Trigger.TriggerHandler.add("timestamp","UNWIND apoc.trigger.nodesByLabel({assignedLabels},'Person') AS n SET n.id = toLower(n.name)",null);
        Trigger.TriggerHandler.add("lowercase","UNWIND {createdNodes} AS n SET n.id = toLower(n.name)",null);
        db.execute("CREATE (f:Person {name:'John Doe'})").close();
        TestUtil.testCall(db, "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node)row.get("f")).getProperty("id"));
            assertEquals("John Doe", ((Node)row.get("f")).getProperty("name"));
        });
    }
    @Test
    public void testTxId() throws Exception {
        Trigger.TriggerHandler.add("txInfo","UNWIND {createdNodes} AS n SET n.txId = {transactionId}, n.txTime = {commitTime}", map("phase","after"));
        db.execute("CREATE (f:Bar)").close();
        TestUtil.testCall(db, "MATCH (f:Bar) RETURN f", (row) -> {
            assertEquals(true, (Long)((Node)row.get("f")).getProperty("txId") > -1L);
            assertEquals(true, (Long)((Node)row.get("f")).getProperty("txTime") > start);
        });
    }

}
