package apoc.ttl;

import apoc.ApocSettings;
import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 21.05.16
 */
public class TTLTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_ttl_schedule, Duration.ofMillis(3000))
            .withSetting(ApocSettings.apoc_ttl_enabled, true)
            .withSetting(ApocSettings.apoc_ttl_batch_size, 8000L);

    @Before
    public void setUp() {
        db.shutdown();
    }

    @Test
    public void testExpire600NodesIn2Steps() throws Exception {
        db.withSetting(ApocSettings.apoc_ttl_limit, 300L);
        restartAndRegister(db);
        db.executeTransactionally("UNWIND range(1,1600) as range CREATE (n:Foo:TTL {id: range, ttl: timestamp() + 100});");
        db.executeTransactionally("UNWIND range(1,2500) as range CREATE (n:Bar:TTL {id: range, ttl: timestamp() + 100});");
        testNodes(1600,2500);
        Thread.sleep(5*1000);
        testNodes(1300,2500);
        Thread.sleep(3*1000);
        testNodes(1000,2500);
    }

    @Test
    public void testExpireOnlyLimitedNumberOfNodes() throws Exception {
        db.withSetting(ApocSettings.apoc_ttl_limit, 500L);
        restartAndRegister(db);
        db.executeTransactionally("UNWIND range(1,1500) as range CREATE (:Baz)-[:REL_TEST]->(n:Foo:TTL {id: range, ttl: timestamp() + 100});");
        db.executeTransactionally("UNWIND range(1,2500) as range CREATE (n:Bar:TTL {id: range, ttl: timestamp() + 100});");
        testNodes(1500,2500);
        Thread.sleep(5*1000);
        testNodes(1000,2500);
    }

    @Test
    public void testExpireAllNodes() throws Exception {
        db.withSetting(ApocSettings.apoc_ttl_limit, 0L);
        restartAndRegister(db);
        db.executeTransactionally("UNWIND range(1,2000) as range CREATE (:Baz)-[:REL_TEST]->(n:Foo:TTL {id: range, ttl: timestamp() + 100});");
        db.executeTransactionally("UNWIND range(1,3000) as range CREATE (n:Bar:TTL {id: range, ttl: timestamp() + 100});");
        testNodes(2000, 3000);
        Thread.sleep(5 * 1000);
        testNodes(0, 0);
    }

    // test extracted from apoc.date
    @Test
    public void testExpire() throws Exception {
        restartAndRegister(db);
        db.executeTransactionally("CREATE (n:Foo:TTL) SET n.ttl = timestamp() + 100");
        db.executeTransactionally("CREATE (n:Bar) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)");
        testNodes(1,1);
        Thread.sleep(10*1000);
        testNodes(0,0);
    }

    private static void testNodes(int foo, int bar) {
        try (Transaction tx=db.beginTx()) {
            assertEquals(foo, Iterators.count(tx.findNodes(Label.label("Foo"))));
            assertEquals(bar, Iterators.count(tx.findNodes(Label.label("Bar"))));
            assertEquals(foo + bar, Iterators.count(tx.findNodes(Label.label("TTL"))));
            tx.commit();
        }
    }

    private static void restartAndRegister(DbmsRule db) throws Exception {
        db.restartDatabase();
        TestUtil.registerProcedure(db, TTL.class, Periodic.class);
    }
}
