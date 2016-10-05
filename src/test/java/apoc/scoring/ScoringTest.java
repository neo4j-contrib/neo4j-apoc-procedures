package apoc.scoring;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 05.10.16
 */
public class ScoringTest {

    private static GraphDatabaseService db;
    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Scoring.class);
    }
    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void existence() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.scoring.existence(10,true) as score", (row) -> assertEquals(10D,row.get("score")));
        TestUtil.testCall(db, "RETURN apoc.scoring.existence(10,false) as score", (row) -> assertEquals(0D,row.get("score")));
    }

    @Test
    public void pareto() throws Exception {
        TestUtil.testResult(db, "UNWIND [0,1,2,8,10,100] as value RETURN value, apoc.scoring.pareto(2,8,10,value) as score", (r) -> {
            assertEquals(0d,r.next().get("score"));
            assertEquals(0d,r.next().get("score"));
            assertEquals(3.3d,(double)r.next().get("score"),0.1d);
            assertEquals(8d,r.next().get("score"));
            assertEquals(8.7,(double)r.next().get("score"),0.1d);
            assertEquals(10d,(double)r.next().get("score"),0.1d);
        });
    }

}
