package apoc.date;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocSettings.apoc_ttl_enabled;
import static apoc.ApocSettings.apoc_ttl_schedule;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 21.05.16
 */
public class TTLTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(apoc_ttl_schedule, "5")
            .withSetting(apoc_ttl_enabled, "true");

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Date.class);
        db.execute("CREATE (n:Foo:TTL) SET n.ttl = timestamp() + 100").close();
        db.execute("CREATE (n:Bar) WITH n CALL apoc.date.expireIn(n,500,'ms') RETURN count(*)").close();
        testNodes(1,1);
    }

    @Test
    public void testExpire() throws Exception {
        Thread.sleep(10*1000);
        testNodes(0,0);
    }

    private static void testNodes(int foo, int bar) {
        try (Transaction tx=db.beginTx()) {
            assertEquals(foo, Iterators.count(db.findNodes(Label.label("Foo"))));
            assertEquals(bar, Iterators.count(db.findNodes(Label.label("Bar"))));
            assertEquals(foo + bar, Iterators.count(db.findNodes(Label.label("TTL"))));
            tx.success();
        }
    }
}
