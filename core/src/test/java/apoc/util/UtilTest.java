package apoc.util;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 19.05.16
 */
public class UtilTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static Node node;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Utils.class);
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode(Label.label("User"));
            node.setProperty("name", "foo");
            tx.commit();
        }
    }

    @Test
    public void testSubMap() throws Exception {
        assertEquals(map("b","c"),Util.subMap(map("a.b","c"),"a"));
        assertEquals(map("b","c"),Util.subMap(map("a.b","c"),"a."));
        assertEquals(map(),Util.subMap(map("a.b","c"),"x"));
    }

    @Test
    public void testPartitionList() throws Exception {
        List list = asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertEquals(1,Util.partitionSubList(list,0).count());
        assertEquals(1,Util.partitionSubList(list,1).count());
        assertEquals(2,Util.partitionSubList(list,2).count());
        assertEquals(3,Util.partitionSubList(list,3).count());
        assertEquals(4,Util.partitionSubList(list,4).count());
        assertEquals(5,Util.partitionSubList(list,5).count());
        assertEquals(5,Util.partitionSubList(list,6).count());
        assertEquals(5,Util.partitionSubList(list,7).count());
        assertEquals(5,Util.partitionSubList(list,8).count());
        assertEquals(5,Util.partitionSubList(list,9).count());
        assertEquals(10,Util.partitionSubList(list,10).count());
        assertEquals(10,Util.partitionSubList(list,11).count());
        assertEquals(10,Util.partitionSubList(list,20).count());
    }

    @Test
    public void cleanPassword() throws Exception {
        String url = "http://%slocalhost:7474/path?query#ref";
        assertEquals(format(url,""), Util.cleanUrl(format(url, "user:pass@")));
    }

    @Test
    public void mergeNullMaps() {
        assertNotNull(Util.merge(null, null));
    }

    @Test(expected = QueryExecutionException.class)
    public void testValidateQuery() {
        try {
            Util.validateQuery(db, "Match (n) return m");
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains("Variable `m` not defined"));
            throw e;
        }
    }

    @Test
    public void testSanitize() {
        assertEquals("``", Util.sanitize("`"));
        assertEquals("``", Util.sanitize("\u0060"));
        assertEquals("``", Util.sanitize("``"));
        assertEquals("````", Util.sanitize("```"));
        assertEquals("````", Util.sanitize("\u0060\u0060\u0060"));
        assertEquals("````", Util.sanitize("\\u0060\\u0060\\u0060"));
        assertEquals("Hello``", Util.sanitize("Hello`"));
        assertEquals("Hi````there", Util.sanitize("Hi````there"));
        assertEquals("Hi``````there", Util.sanitize("Hi`````there"));
        assertEquals("``a``b``c``", Util.sanitize("`a`b`c`"));
        assertEquals("``a``b``c``d``", Util.sanitize("\u0060a`b`c\u0060d\u0060"));
        assertEquals("``a``b``c``d``", Util.sanitize("\\u0060a`b`c\\u0060d\\u0060"));
        assertEquals("Foo ``", Util.sanitize("Foo \\u0060"));
        assertEquals("ABC", Util.sanitize("ABC"));
        assertEquals("A C", Util.sanitize("A C"));
        assertEquals("A`` C", Util.sanitize("A` C"));
        assertEquals("ALabel", Util.sanitize("ALabel"));
        assertEquals("A Label", Util.sanitize("A Label"));
        assertEquals("A ``Label", Util.sanitize("A `Label"));
        assertEquals("``A ``Label", Util.sanitize("`A `Label"));
        assertEquals("``A ``Label", Util.sanitize("`A `Label"));
        assertEquals("Spring Data Neo4j⚡️RX", Util.sanitize("Spring Data Neo4j⚡️RX"));
        assertEquals("Foo ``", Util.sanitize("Foo \u0060"));
        assertEquals("Foo``bar", Util.sanitize("Foo\\`bar"));
        assertEquals("Foo\\``bar", Util.sanitize("Foo\\\\`bar"));
        assertEquals("ᑖ", Util.sanitize("ᑖ"));
        assertEquals("⚡️", Util.sanitize("⚡️"));
        assertEquals("uᑖ", Util.sanitize("\\u0075\\u1456"));
        assertEquals("ᑖ", Util.sanitize("\u1456"));
        assertEquals("something\\u005C\\u00751456", Util.sanitize("something\\u005C\\u00751456"));
        assertEquals("``", Util.sanitize("\u005C\\u0060"));
        assertEquals("\\u005C\\u00750060", Util.sanitize("\\u005Cu0060"));
        assertEquals("\\``", Util.sanitize("\\u005C\\u0060"));
        assertEquals("x\\y", Util.sanitize("x\\y"));
        assertEquals("x\\y", Util.sanitize("x\\\\y"));
        assertEquals("x\\\\y", Util.sanitize("x\\\\\\\\y"));
        assertEquals("x``y", Util.sanitize("x\\`y"));
        assertEquals("Foo ``", Util.sanitize("Foo \\u0060"));
    }

    @Test
    public void testMerge() {
        try {
            final ResultTransformer<Object> resultTransformer = res -> res.next().get("count");
            try (final Transaction transaction = db.beginTx()) {
                Util.mergeNode(transaction, Label.label("Test"), null, Pair.of("foo", "bar"));
                transaction.commit();
                final long count = (long) db.executeTransactionally("MATCH (n:Test{foo: 'bar'}) RETURN count(n) AS count", Map.of(),
                        resultTransformer);
                assertEquals(1, count);
            }
            try (final Transaction transaction = db.beginTx()) {
                Util.mergeNode(transaction, Label.label("Test"), Label.label("Bar"), Pair.of("foo", "bar1"));
                transaction.commit();
                final long count = (long) db.executeTransactionally("MATCH (n:Test:Bar{foo: 'bar1'}) RETURN count(n) AS count", Map.of(),
                        resultTransformer);
                assertEquals(1, count);
            }
            final long count = (long) db.executeTransactionally("MATCH (n:Test) RETURN count(n) AS count", Map.of(),
                    resultTransformer);
            assertEquals(2, count);
        } finally {
            db.executeTransactionally("MATCH (n:Test) DETACH DELETE n");
        }
    }
}
