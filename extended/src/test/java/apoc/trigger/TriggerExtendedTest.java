package apoc.trigger;

import apoc.create.Create;
import apoc.nodes.Nodes;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;

/**
 * @author mh
 * @since 20.09.16
 */
public class TriggerExtendedTest {
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(procedure_unrestricted, List.of("apoc*"));

    private long start;

    @Before
    public void setUp() throws Exception {
        start = System.currentTimeMillis();
        TestUtil.registerProcedure(db, Trigger.class, TriggerExtended.class, Nodes.class, Create.class);
        apocConfig().setProperty(APOC_TRIGGER_ENABLED, true);
    }

    @AfterAll
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testTimeStampTriggerForUpdatedProperties() throws Exception {
        db.executeTransactionally("CALL apoc.trigger.add('timestamp','UNWIND apoc.trigger.nodesByLabel($assignedNodeProperties,null) AS n SET n.ts = timestamp()',{})");
        db.executeTransactionally("CREATE (f:Foo) SET f.foo='bar'");
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node)row.get("f")).hasProperty("ts"));
        });
    }

    @Test
    public void testLowerCaseName() throws Exception {
        db.executeTransactionally("create constraint for (p:Person) require p.id is unique");
        db.executeTransactionally("CALL apoc.trigger.add('lowercase','UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n.id = toLower(n.name)',{})");
        db.executeTransactionally("CREATE (f:Person {name:'John Doe'})");
        TestUtil.testCall(db, "MATCH (f:Person) RETURN f", (row) -> {
            assertEquals("john doe", ((Node)row.get("f")).getProperty("id"));
            assertEquals("John Doe", ((Node)row.get("f")).getProperty("name"));
        });
    }
    @Test
    public void testSetLabels() throws Exception {
        db.executeTransactionally("CREATE (f {name:'John Doe'})");
        db.executeTransactionally("CALL apoc.trigger.add('setlabels','UNWIND apoc.trigger.nodesByLabel($assignedLabels,\"Person\") AS n SET n:Man',{})");
        db.executeTransactionally("MATCH (f) SET f:Person");
        TestUtil.testCall(db, "MATCH (f:Man) RETURN f", (row) -> {
            assertEquals("John Doe", ((Node)row.get("f")).getProperty("name"));
            assertEquals(true, ((Node)row.get("f")).hasLabel(Label.label("Person")));
        });

        long count = TestUtil.singleResultFirstColumn(db, "MATCH (f:Man) RETURN count(*) as c");
        assertEquals(1L, count);
    }


    @Test
    public void testTxIdAfterAsync() throws Exception {
        db.executeTransactionally("CALL apoc.trigger.add('triggerTest','UNWIND apoc.trigger.propertiesByKey($assignedNodeProperties, \"_executed\") as prop " +
                "	WITH prop.node as n " +
                "	CREATE (z:SON {father:id(n)}) " +
                "	CREATE (n)-[:GENERATED]->(z)', " +
                "{phase:'afterAsync'})");
        db.executeTransactionally("CREATE (:TEST {name:'x', _executed:0})");
        db.executeTransactionally("CREATE (:TEST {name:'y', _executed:0})");
        org.neo4j.test.assertion.Assert.assertEventually(() -> db.executeTransactionally("MATCH p = ()-[r:GENERATED]->() RETURN count(p) AS count",
                Collections.emptyMap(), (r) -> r.<Long>columnAs("count").next()),
                (value) -> value == 2L, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testIssue1152() {
        db.executeTransactionally("CREATE (n:To:Delete {prop1: 'val1', prop2: 'val2'}) RETURN id(n) as id");
        
        testIssue1152Common("before");
        testIssue1152Common("after");
    }

    private void testIssue1152Common(String phase) {
        // we check also that we can execute write operation (through virtualNode functions, e.g. apoc.create.addLabels)
        final String query = "UNWIND $deletedNodes as deletedNode " +
                "WITH apoc.trigger.toNode(deletedNode, $removedLabels, $removedNodeProperties) AS deletedNode " +
                "CREATE (r:Report {id: id(deletedNode)}) WITH r, deletedNode " +
                "CALL apoc.create.addLabels(r, apoc.node.labels(deletedNode)) yield node with node, deletedNode " +
                "set node+=apoc.any.properties(deletedNode)";
        
        db.executeTransactionally("call apoc.trigger.add('issue1152', $query , {phase: $phase})",
                Map.of("query", query, "phase", phase));

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
        db.executeTransactionally("call apoc.trigger.add('myTrigger', $query , {phase: $phase})",
                Map.of("name", UUID.randomUUID().toString(), "query", triggerQuery, "phase", phase));
        db.executeTransactionally("MATCH (:Start)-[r:MY_TYPE]->(:End) DELETE r");

        TestUtil.testCall(db, assertionQuery, (row) -> {
            final Entity n = (Entity) row.get("n");
            assertEquals("MY_TYPE", n.getProperty("type"));
            assertEquals("val1", n.getProperty("prop1"));
            assertEquals("val2", n.getProperty("prop2"));
        });
    }
}
