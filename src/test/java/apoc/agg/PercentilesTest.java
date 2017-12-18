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

public class PercentilesTest {

    private static GraphDatabaseService db;

    @BeforeClass public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Percentiles.class);
    }

    @AfterClass public static void tearDown() {
        db.shutdown();
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
}
