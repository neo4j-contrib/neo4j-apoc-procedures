package apoc.math;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.math.RoundingMode;

import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;

public class MathsTest {

    private static GraphDatabaseService db;
    @BeforeClass public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Maths.class);
    }
    @AfterClass public static void tearDown() {
        db.shutdown();
    }

    @Test public void testRoundMode() throws Exception {
        testRound(0.5, 1.0, RoundingMode.UP);
        testRound(0.5, 1.0, RoundingMode.CEILING);
        testRound(0.5, 1.0, RoundingMode.HALF_UP);
        testRound(0.5, 0.0, RoundingMode.HALF_DOWN);
        testRound(0.5, 0.0, RoundingMode.DOWN);
        testRound(0.5, 0.0, RoundingMode.HALF_EVEN);
        testRound(0.5, 0.0, RoundingMode.FLOOR);

        testRound(-0.5, -1.0, RoundingMode.UP);
        testRound(-0.5, 0.0, RoundingMode.CEILING);
        testRound(-0.5, -1.0, RoundingMode.HALF_UP);
        testRound(-0.5, 0.0, RoundingMode.HALF_DOWN);
        testRound(-0.5, 0.0, RoundingMode.DOWN);
        testRound(-0.5, 0.0, RoundingMode.HALF_EVEN);
        testRound(-0.5, -1.0, RoundingMode.FLOOR);
    }
    @Test public void testRoundPrecision() throws Exception {
        testRound(Math.PI,3.0,0);
        testRound(Math.PI,3.1,1);
        testRound(Math.PI,3.14,2);
        testRound(Math.PI,3.142,3);
        testRound(Math.PI,3.1416,4);
    }
    @Test public void testRoundSimple() throws Exception {
        testRound(1.49, 1.0);
        testRound(1.2, 1.0);
        testRound(1.0, 1.0);
        testRound(0.5, 1.0);
        testRound(-0.5, -1.0);
        testRound(0.3, 0);
        testRound(-0.3, 0);
        testRound(0.0, 0);
    }

    public void testRound(double value, double expected) {
        testCall(db, "RETURN apoc.math.round({value}) as value",map("value",value), (row) -> assertEquals(expected,row.get("value")));
    }
    public void testRound(double value, double expected, int precision) {
        testCall(db, "RETURN apoc.math.round({value},{prec}) as value",map("value",value,"prec",precision), (row) -> assertEquals(expected,row.get("value")));
    }
    public void testRound(double value, double expected, RoundingMode mode) {
        testCall(db, "RETURN apoc.math.round({value},{prec},{mode}) as value",map("value",value,"prec",0,"mode",mode.name()), (row) -> assertEquals(expected,row.get("value")));
    }
}
