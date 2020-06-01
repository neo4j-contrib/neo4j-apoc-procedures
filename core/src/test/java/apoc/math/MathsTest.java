package apoc.math;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.math.RoundingMode;

import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;

public class MathsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass public static void setUp() throws Exception {
        TestUtil.registerProcedure(db,Maths.class);
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
        testCall(db, "RETURN apoc.math.round($value) as value",map("value",value), (row) -> assertEquals(expected,row.get("value")));
    }
    public void testRound(double value, double expected, int precision) {
        testCall(db, "RETURN apoc.math.round($value,$prec) as value",map("value",value,"prec",precision), (row) -> assertEquals(expected,row.get("value")));
    }
    public void testRound(double value, double expected, RoundingMode mode) {
        testCall(db, "RETURN apoc.math.round($value,$prec,$mode) as value",map("value",value,"prec",0,"mode",mode.name()), (row) -> assertEquals(expected,row.get("value")));
    }

    @Test public void testMaxLong(){
        testCall(db, "RETURN apoc.math.maxLong() as max",(row) -> assertEquals(Long.MAX_VALUE,row.get("max")));
    }

    @Test public void testMinLong(){
        testCall(db, "RETURN apoc.math.minLong() as min",(row) -> assertEquals(Long.MIN_VALUE,row.get("min")));
    }

    @Test public void testMaxDouble(){
        testCall(db, "RETURN apoc.math.maxDouble() as max",(row) -> assertEquals(Double.MAX_VALUE,row.get("max")));
    }

    @Test public void testMinDouble(){
        testCall(db, "RETURN apoc.math.minDouble() as min",(row) -> assertEquals(Double.MIN_VALUE,row.get("min")));
    }

    @Test public void testMaxInt(){
        testCall(db, "RETURN apoc.math.maxInt() as max",(row) -> assertEquals(Long.valueOf(Integer.MAX_VALUE), row.get("max")));
    }

    @Test public void testMinInt(){
        testCall(db, "RETURN apoc.math.minInt() as min",(row) -> assertEquals(Long.valueOf(Integer.MIN_VALUE),row.get("min")));
    }

    @Test public void testMaxByte(){
        testCall(db, "RETURN apoc.math.maxByte() as max",(row) -> assertEquals(Long.valueOf(Byte.MAX_VALUE), row.get("max")));
    }

    @Test public void testMinByte(){
        testCall(db, "RETURN apoc.math.minByte() as min",(row) -> assertEquals(Long.valueOf(Byte.MIN_VALUE),row.get("min")));
    }
}
