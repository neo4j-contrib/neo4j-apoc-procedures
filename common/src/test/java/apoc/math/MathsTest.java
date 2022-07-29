package apoc.math;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;


import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MathsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass public static void setUp() throws Exception {
        TestUtil.registerProcedure(db,Maths.class);
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
    
    @Test
    public void testSigmoid() {
        testCall(db, "RETURN apoc.math.sigmoid(2.5) as value",(row) -> assertEquals(0.92D, (double) row.get("value"), 0.01D));
        testCall(db, "RETURN apoc.math.sigmoid(null) as value",(row) -> assertNull(row.get("value")));
    }
    
    @Test
    public void testSigmoidPrime() {
        testCall(db, "RETURN apoc.math.sigmoidPrime(2.5) as value",(row) -> assertEquals(0.07D, (double) row.get("value"), 0.01D));
        testCall(db, "RETURN apoc.math.sigmoidPrime(null) as value",(row) -> assertNull(row.get("value")));
    }
    
    @Test
    public void testHyperbolicTan() {
        testCall(db, "RETURN apoc.math.tanh(1.5) as value",(row) -> assertEquals(0.90D, (double) row.get("value"), 0.01D));
        testCall(db, "RETURN apoc.math.tanh(null) as value",(row) -> assertNull(row.get("value")));
    }
    
    @Test
    public void testHyperbolicCotan() {
        testCall(db, "RETURN apoc.math.coth(3.5) as value",(row) -> assertEquals(1.00D, (double) row.get("value"), 0.01D));
        testCall(db, "RETURN apoc.math.coth(0) as value",(row) -> assertNull(row.get("value")));
        testCall(db, "RETURN apoc.math.coth(null) as value",(row) -> assertNull(row.get("value")));
    }
    
    @Test
    public void testHyperbolicSin() {
        testCall(db, "RETURN apoc.math.sinh(1.5) as value",(row) -> assertEquals(2.13D, (double) row.get("value"), 0.01D));
        testCall(db, "RETURN apoc.math.sinh(null) as value",(row) -> assertNull(row.get("value")));
    }

    @Test
    public void testHyperbolicCos() {
        testCall(db, "RETURN apoc.math.cosh(1.5) as value",(row) -> assertEquals(2.35D, (double) row.get("value"), 0.01D));
        testCall(db, "RETURN apoc.math.cosh(null) as value",(row) -> assertNull(row.get("value")));
    }
    
    @Test
    public void testHyperbolicSecant() {
        testCall(db, "RETURN apoc.math.sech(1.5) as value",(row) -> assertEquals(0.43D, (double) row.get("value"), 0.01D));
        testCall(db, "RETURN apoc.math.sech(null) as value",(row) -> assertNull(row.get("value")));
    }

    @Test
    public void testHyperbolicCosecant() {
        testCall(db, "RETURN apoc.math.csch(1.5) as value",(row) -> assertEquals(0.47D, (double) row.get("value"), 0.01D));
        testCall(db, "RETURN apoc.math.csch(0) as value",(row) -> assertNull(row.get("value")));
        testCall(db, "RETURN apoc.math.csch(null) as value",(row) -> assertNull(row.get("value")));
    }
}
