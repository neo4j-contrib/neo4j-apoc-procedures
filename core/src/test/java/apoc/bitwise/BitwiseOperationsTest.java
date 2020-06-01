package apoc.bitwise;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class BitwiseOperationsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public static final String BITWISE_CALL = "return apoc.bitwise.op($a,$op,$b) as value";

    private int a,b;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, BitwiseOperations.class);
    }

    public void testOperation(String op, long expected) {
        Map<String, Object> params = map("a", a, "op", op, "b", b);
        testCall(db, BITWISE_CALL, params,
                (row) -> assertEquals("operation " + op, expected, row.get("value")));
    }

    @Test
    public void testOperations() throws Throwable {
        a = 0b0011_1100;
        b = 0b0000_1101;
        testOperation("&", 12L);
        testOperation("AND", 12L);
        testOperation("OR", 61L);
        testOperation("|", 61L);
        testOperation("^", 49L);
        testOperation("XOR", 49L);
        testOperation("~", -61L);
        testOperation("NOT", -61L);
    }

    @Test
    public void testOperations2() throws Throwable {
        a = 0b0011_1100;
        b = 2;
        testOperation("<<", 240L);
        testOperation("left shift", 240L);
        testOperation(">>", 15L);
        testOperation("right shift", 15L);
        testOperation("right shift unsigned", 15L);
        testOperation(">>>", 15L);
    }
}
