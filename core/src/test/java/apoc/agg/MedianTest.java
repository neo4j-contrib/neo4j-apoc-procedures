package apoc.agg;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class MedianTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Median.class);
    }

    @Test
    public void testMedian() throws Exception {
        testCall(db, "UNWIND [] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(null, row.get("p"));
                });
        testCall(db, "UNWIND [0,1,2,3] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(1.5D, row.get("p"));
                });
        testCall(db, "UNWIND [0,1, 2 ,3,4] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(2D, row.get("p"));
                });
        testCall(db, "UNWIND [1,1.5, 2,2.5 ,3, 3.5] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(2.25D, row.get("p"));
                });
        testCall(db, "UNWIND [1,1.5,2,2.5,3] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(2D, row.get("p"));
                });
    }
}
