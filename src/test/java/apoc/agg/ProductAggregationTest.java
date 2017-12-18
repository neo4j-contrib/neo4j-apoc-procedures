package apoc.agg;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ProductAggregationTest {

    private static GraphDatabaseService db;

    @BeforeClass public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Product.class);
    }

    @AfterClass public static void tearDown() {
        db.shutdown();
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
