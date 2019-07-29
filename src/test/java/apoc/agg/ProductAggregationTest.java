package apoc.agg;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class ProductAggregationTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Product.class);
    }

    @Test
    public void testProduct() throws Exception {
        testCall(db, "UNWIND [] as value RETURN apoc.agg.product(value) as p",
                (row) -> {
                    assertEquals(0D, row.get("p"));
                });
        testCall(db, "UNWIND RANGE(0,3) as value RETURN apoc.agg.product(value) as p",
                (row) -> {
                    assertEquals(0L, row.get("p"));
                });
        testCall(db, "UNWIND RANGE(1,3) as value RETURN apoc.agg.product(value) as p",
                (row) -> {
                    assertEquals(6L, row.get("p"));
                });
        testCall(db, "UNWIND RANGE(2,6) as value RETURN apoc.agg.product(value/2.0) as p",
                (row) -> {
                    assertEquals(22.5D, row.get("p"));
                });
    }
}
