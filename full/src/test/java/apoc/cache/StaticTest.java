package apoc.cache;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 22.05.16
 */
public class StaticTest {
    public static final String VALUE = "testValue";

    @Rule
    public final ProvideSystemProperty systemPropertyRule
            = new ProvideSystemProperty("apoc.static.test", VALUE)
            .and("apoc.static.all.test", VALUE);

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, Collections.singletonList("apoc.*"));

    @Before
    public  void setUp() throws Exception {
        TestUtil.registerProcedure(db, Static.class);
        Static.clear();
    }

    @Test
    public void testGetAllFromConfig() throws Exception {
        TestUtil.testCall(db, "return apoc.static.getAll('all') as value", r -> assertEquals(map("test",VALUE),r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('all.test2',42)", r -> assertNull(r.get("value")));
        TestUtil.testCall(db, "return apoc.static.getAll('all') as value", r -> assertEquals(map("test",VALUE,"test2",42L),r.get("value")));
        TestUtil.testCall(db, "return apoc.static.getAll('') as value", r -> assertEquals(map("test",VALUE,"all.test",VALUE,"all.test2",42L),r.get("value")));
    }

    @Test
    public void testListFromConfig() throws Exception {
        TestUtil.testCall(db, "call apoc.static.set('all.test2',42)", r -> assertNull(r.get("value")));
        TestUtil.testResult(db, "call apoc.static.list('all') yield key, value return * order by key", r -> {
            assertEquals(map("key","test","value",VALUE),r.next());
            assertEquals(map("key","test2","value",42L),r.next());
            assertFalse(r.hasNext());
        });
        TestUtil.testResult(db, "call apoc.static.list('') yield key, value return * order by key", r -> {
            assertEquals(map("key","all.test","value",VALUE),r.next());
            assertEquals(map("key","all.test2","value",42L),r.next());
            assertEquals(map("key","test","value",VALUE),r.next());
            assertFalse(r.hasNext());
        });
    }

    @Test
    public void testOverrideConfig() throws Exception {
        TestUtil.testCall(db, "call apoc.static.get('test')", r -> assertEquals(VALUE,r.get("value")));
        TestUtil.testCall(db, "return apoc.static.get('test') as value", r -> assertEquals(VALUE,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('test',42)", r -> assertEquals(VALUE,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.get('test')", r -> assertEquals(42L,r.get("value")));
        TestUtil.testCall(db, "return apoc.static.get('test') as value", r -> assertEquals(42L,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('test',null)", r -> assertEquals(42L,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.get('test')", r -> assertEquals(VALUE,r.get("value")));
        TestUtil.testCall(db, "return apoc.static.get('test') as value", r -> assertEquals(VALUE,r.get("value")));
    }

    @Test
    public void testSet() throws Exception {
        TestUtil.testCall(db, "call apoc.static.get('test2')", r -> assertNull(r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('test2',42)", r -> assertNull(r.get("value")));
        TestUtil.testCall(db, "call apoc.static.get('test2')", r -> assertEquals(42L,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.set('test2',null)", r -> assertEquals(42L,r.get("value")));
        TestUtil.testCall(db, "call apoc.static.get('test2')", r -> assertNull(r.get("value")));
    }
}
