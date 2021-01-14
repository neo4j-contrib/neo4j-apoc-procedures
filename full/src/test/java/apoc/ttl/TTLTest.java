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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TTLTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_ttl_schedule, Duration.ofMillis(3000))
            .withSetting(ApocSettings.apoc_ttl_enabled, true);

    @Before
    public void setUp() {
        db.shutdown();
    }

    @Test
    public void testExpireManyNodes() throws Exception {
        restartAndRegister(db);
        db.executeTransactionally("UNWIND range(1,200000) as range CREATE (:Baz)-[:REL_TEST]->(n:Foo:TTL {id: range, ttl: timestamp() + 100});");
        db.executeTransactionally("UNWIND range(1,300000) as range CREATE (n:Bar:TTL {id: range, ttl: timestamp() + 100});");
        assertTrue(isNodeCountConsistent(200000, 300000));
        org.neo4j.test.assertion.Assert.assertEventually(() -> isNodeCountConsistent(0, 0), (value) -> value, 30L, TimeUnit.SECONDS);
    }

    // test extracted from apoc.date
    @Test
    public void testExpire() throws Exception {
        restartAndRegister(db);
        db.executeTransactionally("CREATE (n:Foo:TTL) SET n.ttl = timestamp() + 100");
        db.executeTransactionally("CREATE (n:Bar) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)");
        assertTrue(isNodeCountConsistent(1,1));
        org.neo4j.test.assertion.Assert.assertEventually(() -> isNodeCountConsistent(0, 0), (value) -> value, 10L, TimeUnit.SECONDS);
    }

    private static boolean isNodeCountConsistent(int foo, int bar) {
        try (Transaction tx = db.beginTx()) {
            boolean isNotCountConsistent = foo == Iterators.count(tx.findNodes(Label.label("Foo")))
                    && bar == Iterators.count(tx.findNodes(Label.label("Bar")))
                    && foo + bar == Iterators.count(tx.findNodes(Label.label("TTL")));
            tx.commit();
            return isNotCountConsistent;
        }
    }

    private static void restartAndRegister(DbmsRule db) throws Exception {
        db.restartDatabase();
        TestUtil.registerProcedure(db, TTL.class, Periodic.class);
    }
}
