package apoc.bitwise;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import apoc.util.TestUtil;

public class BitwiseOperationsTest {
    private static GraphDatabaseService db;
    private int a = 60;	/* 60 = 0011 1100 */  
    private int b = 13;	/* 13 = 0000 1101 */
   

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
		
	
	@Test
	public void testBitwiseAnd() throws Throwable {
		String query = "call apoc.bitwise.cmp(" + a + ",'&'," + b + ") yield value as c return c";
		TestUtil.testCall(db, query, (row) -> assertEquals(12L,row.get("c")));
	}

	@Test
	public void testBitwiseOr() throws Throwable {
		String query = "call apoc.bitwise.cmp(" + a + ",'|'," + b + ") yield value as c return c";
		TestUtil.testCall(db, query, (row) -> assertEquals(61L,row.get("c")));
	}

	@Test
	public void testBitwiseXOr() throws Throwable {
		String query = "call apoc.bitwise.cmp(" + a + ",'^'," + b + ") yield value as c return c";
		TestUtil.testCall(db, query, (row) -> assertEquals(49L,row.get("c")));
	}

	@Test
	public void testBitwiseNot() throws Throwable {
		String query = "call apoc.bitwise.cmp(" + a + ",'~'," + b + ") yield value as c return c";
		TestUtil.testCall(db, query, (row) -> assertEquals(-61L,row.get("c")));
	}

	@Test
	public void testBitwiseShiftLeft() throws Throwable {
		String query = "call apoc.bitwise.cmp(" + a + ",'<<',2) yield value as c return c";
		TestUtil.testCall(db, query, (row) -> assertEquals(240L,row.get("c")));
	}
	@Test
	public void testBitwiseShiftRight() throws Throwable {
		String query = "call apoc.bitwise.cmp(" + a + ",'>>',2) yield value as c return c";
		TestUtil.testCall(db, query, (row) -> assertEquals(15L,row.get("c")));
	}
	@Test
	public void testBitwiseShiftRightRight() throws Throwable {
		String query = "call apoc.bitwise.cmp(" + a + ",'>>>',2) yield value as c return c";
		TestUtil.testCall(db, query, (row) -> assertEquals(15L,row.get("c")));
	}
	
}
