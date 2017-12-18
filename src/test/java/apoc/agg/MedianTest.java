package apoc.agg;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class MedianTest {

    private static GraphDatabaseService db;

    @BeforeClass public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Median.class);
    }

    @AfterClass public static void tearDown() {
        db.shutdown();
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
