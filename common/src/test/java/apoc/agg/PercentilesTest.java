package apoc.agg;

import apoc.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class PercentilesTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Percentiles.class);
    }

    @Test
    public void testPercentiles() throws Exception {
        testCall(db, "UNWIND [] as value RETURN apoc.agg.percentiles(value) as p",
                (row) -> {
                    assertEquals(asList(null,null,null,null,null,null), row.get("p"));
                });
        testCall(db, "UNWIND [0,1,1,2,2,2,3] as value RETURN apoc.agg.percentiles(value,[0.5,0.95]) as p",
                (row) -> {
                    assertEquals(asList(2L,3L), row.get("p"));
                });
        testCall(db, "UNWIND [1,1,1,1,2,2,3,4] as value RETURN apoc.agg.percentiles(value) as p",
                (row) -> {
                    assertEquals(asList(1L,2L,3L,4L,4L), row.get("p"));
                });
        testCall(db, "UNWIND [1,1,1,1,2,2,3,4] as value RETURN apoc.agg.percentiles(value,[0.5,0.75,0.9,0.95,0.99]) as p",
                (row) -> {
                    assertEquals(asList(1L,2L,3L,4L,4L), row.get("p"));
                });
    }
    @Test
    public void testPercentilesDoubles() throws Exception {
        testCall(db, "UNWIND [] as value RETURN apoc.agg.percentiles(value) as p",
                (row) -> {
                    assertEquals(asList(null,null,null,null,null,null), row.get("p"));
                });
        testCall(db, "UNWIND [0,1,1,2.0,2,2,3] as value RETURN apoc.agg.percentiles(value,[0.5,0.95]) as p",
                (row) -> {
                    assertSameValues(asList(2D,3D), row.get("p"));
                });
        testCall(db, "UNWIND [1,1,1.0,1,2,2,3,4] as value RETURN apoc.agg.percentiles(value) as p",
                (row) -> {
                    assertSameValues(asList(1D,2D,3D,4D,4D), row.get("p"));
                });
        testCall(db, "UNWIND [1,1,1,1,2,2,3,4.0] as value RETURN apoc.agg.percentiles(value,[0.5,0.75,0.9,0.95,0.99]) as p",
                (row) -> {
                    assertSameValues(asList(1D,2D,3D,4D,4D), row.get("p"));
                });
    }

    private static void assertSameValues(List<Double> expected, Object values) {
        List<Double> doubleValues = (List<Double>) values;
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(expected.get(i), doubleValues.get(i),0.0001);
        }
    }
}
