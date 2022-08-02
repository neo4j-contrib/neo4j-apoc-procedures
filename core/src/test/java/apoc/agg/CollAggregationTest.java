package apoc.agg;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class CollAggregationTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, CollAggregation.class);
    }

    @Test
    public void testNth() throws Exception {
        testCall(db, "UNWIND RANGE(0,10) as value RETURN apoc.agg.nth(value, 0) as first, apoc.agg.nth(value, 3) as third,apoc.agg.nth(value, -1) as last",
                (row) -> {
                    assertEquals(0L, row.get("first"));
                    assertEquals(3L, row.get("third"));
                    assertEquals(10L, row.get("last"));
                });
    }
    @Test
    public void testFirst() throws Exception {
        testCall(db, "UNWIND RANGE(0,10) as value RETURN apoc.agg.first(value) as first",
                (row) -> {
                    assertEquals(0L, row.get("first"));
                });
        testCall(db, "UNWIND [null,42,43,null] as value RETURN apoc.agg.first(value) as first",
                (row) -> {
                    assertEquals(42L, row.get("first"));
                });
    }
    @Test
    public void testLast() throws Exception {
        testCall(db, "UNWIND RANGE(0,10) as value RETURN apoc.agg.last(value) as last",
                (row) -> {
                    assertEquals(10L, row.get("last"));
                });
        testCall(db, "UNWIND [null,41,null,42,null] as value RETURN apoc.agg.last(value) as last",
                (row) -> {
                    assertEquals(42L, row.get("last"));
                });
    }
    @Test
    public void testSlice() throws Exception {
        testCall(db, "UNWIND RANGE(0,10) as value RETURN apoc.agg.slice(value,1,3) as slice",
                (row) -> {
                    assertEquals(asList(1L,2L,3L), row.get("slice"));
                });
        testCall(db, "UNWIND RANGE(0,3) as value RETURN apoc.agg.slice(value) as slice",
                (row) -> {
                    assertEquals(asList(0L,1L,2L,3L), row.get("slice"));
                });
        testCall(db, "UNWIND RANGE(0,3) as value RETURN apoc.agg.slice(value,2) as slice",
                (row) -> {
                    assertEquals(asList(2L,3L), row.get("slice"));
                });
        testCall(db, "UNWIND [null,41,null,42,null,43,null] as value RETURN apoc.agg.slice(value,1,3) as slice",
                (row) -> {
                    assertEquals(asList(42L,43L), row.get("slice"));
                });
    }
}
