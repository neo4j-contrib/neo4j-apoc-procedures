package apoc.agg;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;

public class StatisticsTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Statistics.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testStatistics() throws Exception {
        testCall(db, "UNWIND [] as value RETURN apoc.agg.statistics(value) as p",
                (row) -> {

                    assertEquals(map("min", null, "max", null, "minNonZero", Long.valueOf(Long.MAX_VALUE).doubleValue(), "total", 0L, "stdev", 0D, "mean", 0D), row.get("p"));
                });
        testCall(db, "UNWIND [0,1,1,2,2,2,3] as value RETURN apoc.agg.statistics(value,[0.5,0.95]) as p",
                (row) -> {
                    assertEquals(map("total", 7L, "min", 0L, "minNonZero", 1D, "max", 3L, "mean", 1.5714285714285714D, "0.5", 2L, "0.95", 3L, "stdev", 0.9035079029052512), row.get("p"));
                });
    }

    @Test
    public void testStatisticsDouble() throws Exception {
        testCall(db, "UNWIND [0,1,1,2.0,2,2,3] as value RETURN apoc.agg.statistics(value,[0.5,0.95]) as p",
                (row) -> {
                    assertEquals(map("total", 7L, "min", 0L, "minNonZero", 1D, "max", 3L, "mean", 1.5714329310825892D, "0.5", 2.0000076293945312D, "0.95", 3.0000076293945312D, "stdev", 0.9035111771858774), row.get("p"));
                });
    }

    @Test
    public void testStatisticsDoubleMinMax() throws Exception {
        testCall(db, "UNWIND [0.123,0.234] as value RETURN apoc.agg.statistics(value,[0.05,0.5,0.95]) as p",
                (row) -> {
                    Map<String, Number> stats = (Map<String, Number>) row.get("p");
                    assertEquals(0.234D, stats.get("max"));
                    assertEquals(0.123D, stats.get("min"));
                });
    }
}
