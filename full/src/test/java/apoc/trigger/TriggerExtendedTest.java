package apoc.trigger;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static apoc.ApocSettings.apoc_trigger_enabled;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 20.09.16
 */
public class TriggerExtendedTest {
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(apoc_trigger_enabled, true);  // need to use settings here, apocConfig().setProperty in `setUp` is too late

    private long start;

    @Before
    public void setUp() throws Exception {
        start = System.currentTimeMillis();
        TestUtil.registerProcedure(db, Trigger.class, TriggerExtended.class);
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
        db.executeTransactionally("create constraint on (p:Person) assert p.id is unique");
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

}
