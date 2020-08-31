package apoc.ttl;

import apoc.date.DateExpiry;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
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

import static apoc.ApocConfig.*;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 21.05.16
 */
public class TTLTest {

    public static DbmsRule db = new ImpermanentDbmsRule();

    public static ProvideSystemProperty systemPropertyRule
            = new ProvideSystemProperty(APOC_TTL_ENABLED, "true")
            .and(APOC_TTL_SCHEDULE, "5")
            .and(APOC_TTL_LIMIT, "0");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(systemPropertyRule).around(db);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, TTL.class);
    }

    @Test
    public void testExpireMoreThanOneThousand() throws Exception {
        db.executeTransactionally("UNWIND range(1,2000) as range CREATE (n:Foo:TTL {id: range, ttl: timestamp() + 100});");
        db.executeTransactionally("UNWIND range(1,3000) as range CREATE (n:Bar:TTL {id: range, ttl: timestamp() + 100});");
        testNodes(2000,3000);
        Thread.sleep(10*1000);
        testNodes(0, 0);
    }

    @Test
    public void testExpire() throws Exception {
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
}
