package apoc.bitwise;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

import apoc.util.TestUtil;

public class BitwiseOperationsTest {
    private static GraphDatabaseService db;
    public static final String BITWISE_CALL = "call apoc.bitwise.op({a},{op},{b})";

    private int a = 0b0011_1100;
    private int b = 0b0000_1101;


    public BitwiseOperationsTest() throws Exception {
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, BitwiseOperations.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }


    public void testOperation(String op, long expected) {
        testCall(db, BITWISE_CALL, map("a", a, "op", op, "b", b), (row) -> assertEquals("operation " + op, expected, row.get("value")));
    }


    @Test
    public void testOperations() throws Throwable {
        testOperation("&", 12L);
        testOperation("AND", 12L);
        testOperation("OR", 61L);
        testOperation("|", 61L);
        testOperation("^", 49L);
        testOperation("XOR", 49L);
        testOperation("~", -61L);
        testOperation("NOT", -61L);
        b = 2;
        testOperation("<<", 240L);
        testOperation("left shift", 240L);
        testOperation(">>", 15L);
        testOperation("right shift", 15L);
        testOperation("right shift unsigned", 15L);
        testOperation(">>>", 15L);
    }
}
