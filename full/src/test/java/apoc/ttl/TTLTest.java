package apoc.ttl;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.APOC_TTL_ENABLED;
import static apoc.ApocConfig.APOC_TTL_SCHEDULE;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 21.05.16
 */
public class TTLTest {

    public static DbmsRule db = new ImpermanentDbmsRule();

    public static ProvideSystemProperty systemPropertyRule
            = new ProvideSystemProperty(APOC_TTL_ENABLED, "true")
            .and(APOC_TTL_SCHEDULE, "5");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(systemPropertyRule).around(db);

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, TTL.class);
        db.executeTransactionally("CREATE (n:Foo:TTL) SET n.ttl = timestamp() + 100");
        testNodes(1, 1,0, 0);
    }

    @Test
    public void testRemoveNodesAndCreateAfterCallProcedure() throws Exception {
        db.executeTransactionally("UNWIND range(1,2000) as range CREATE (n:Baz {id: range});");
        testNodes(1,1,0, 2000);
        db.executeTransactionally("MATCH (n:Baz) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)");
        testNodes(2001, 1,0, 2000);
        db.executeTransactionally("CREATE (n:Baz) SET n.id = '9999'");
        testNodes(2001,1,0, 2001);
        Thread.sleep(10*1000);
        testNodes(0, 0,0, 1);
        db.executeTransactionally("MATCH (n:Baz) delete n");
    }

    @Test
    public void testRemoveThousandNodesWithExpireIn() throws Exception {
        db.executeTransactionally("UNWIND range(1,2000) as range CREATE (n:Baz {id: range});");
        testNodes(1,1,0, 2000);
        db.executeTransactionally("MATCH (n:Baz) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)");
        testNodes(2001, 1,0, 2000);
        Thread.sleep(10*1000);
        testNodes(0, 0,0, 0);
    }

    @Test
    public void testRemoveThousandNodesWithExpire() throws Exception {
        db.executeTransactionally("UNWIND range(1,2000) as range CREATE (n:Baz {id: range});");
        testNodes(1, 1,0, 2000);
        db.executeTransactionally("MATCH (n:Baz) WITH n CALL apoc.ttl.expire(n,timestamp() + 500,'ms') RETURN count(*)");
        testNodes(2001, 1,0, 2000);
        Thread.sleep(10*1000);
        testNodes(0, 0,0, 0);
    }

    @Test
    public void testExpireNodeWithTTLLabel() throws Exception {
        db.executeTransactionally("CREATE (n:Bar) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)");
        Thread.sleep(10*1000);
        testNodes(0, 0,0, 0);
    }

    private static void testNodes(int totalTTL, int foo, int bar, int baz) {
        try (Transaction tx=db.beginTx()) {
            assertEquals(foo, Iterators.count(tx.findNodes(Label.label("Foo"))));
            assertEquals(bar, Iterators.count(tx.findNodes(Label.label("Bar"))));
            assertEquals(baz, Iterators.count(tx.findNodes(Label.label("Baz"))));
            assertEquals(totalTTL, Iterators.count(tx.findNodes(Label.label("TTL"))));
            tx.commit();
        }
    }
}
