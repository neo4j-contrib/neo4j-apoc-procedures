package apoc.date;

import apoc.ttl.TTL;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.Map;

import static apoc.ApocConfig.APOC_TTL_ENABLED;
import static apoc.ApocConfig.APOC_TTL_SCHEDULE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

/**
 * @author mh
 * @since 21.05.16
 */
public class TTLTest {

    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(procedure_unrestricted, Collections.singletonList("apoc.*"));

    public static ProvideSystemProperty systemPropertyRule
            = new ProvideSystemProperty(
                    APOC_TTL_ENABLED, "true")
            .and(APOC_TTL_SCHEDULE, "5")
            .and("apoc.ttl.limit", "200");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(systemPropertyRule).around(db);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, DateExpiry.class);
        TestUtil.registerProcedure(db, TTL.class);
        db.executeTransactionally("CREATE (n:Foo:TTL) SET n.ttl = timestamp() + 100");
        db.executeTransactionally("CREATE (n:Bar) WITH n CALL apoc.date.expireIn(n,500,'ms') RETURN count(*)");
        testNodes(1,1);
    }

    @Test
    public void testExpire() throws Exception {
        Thread.sleep(10*1000);
        testNodes(0,0);
    }

    @Test
    public void testConfig() throws Exception {
        db.executeTransactionally("RETURN apoc.ttl.config() AS value", MapUtil.map(), new ResultTransformer<Object>() {
            @Override
            public Object apply(Result result) {
                Map<String, Object> row = result.next();
                Map<String, Object> value = (Map<String, Object>) row.get("value");
                assertTrue((Boolean) value.get("enabled"));
                assertEquals(5L, value.get("schedule"));
                assertEquals(200L, value.get("limit"));
                return null;
            }
        });
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
