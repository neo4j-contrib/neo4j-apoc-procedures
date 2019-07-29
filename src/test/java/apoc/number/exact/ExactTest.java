package apoc.number.exact;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

/**
 * @author AgileLARUS
 *
 * @since 17 May 2017
 */
public class ExactTest {

	@Rule
	public ExpectedException expected = ExpectedException.none();

	@ClassRule
	public static DbmsRule db = new ImpermanentDbmsRule();

	@BeforeClass public static void sUp() throws Exception {
		TestUtil.registerProcedure(db, Exact.class);
	}

	@Test
	public void testAdd(){
		testCall(db,"return apoc.number.exact.add('1213669989','1238126387') as value", row -> assertEquals(new String("2451796376"), row.get("value")));
	}

	public void testAddNull(){
		testCall(db,"return apoc.number.exact.add(null,'1238126387') as value", row -> assertEquals(null, row.get("value")));
	}

	@Test
	public void testSub(){
		testCall(db,"return apoc.number.exact.sub('1238126387','1213669989') as value", row -> assertEquals(new String("24456398"), row.get("value")));
	}

	@Test
	public void testMul(){
		testCall(db,"return apoc.number.exact.mul('550058444','662557', 15, 'HALF_DOWN') as value", row -> assertEquals(new String("364445072481308"), row.get("value")));
	}

	@Test
	public void testDiv(){
		testCall(db,"return apoc.number.exact.div('550058444','662557', 18, 'HALF_DOWN') as value", row -> assertEquals(new String("830.205467605051339"), row.get("value")));
	}

	@Test
	public void testToInteger(){
		testCall(db,"return apoc.number.exact.toInteger('504238974', 5, 'HALF_DOWN') as value", row -> assertEquals(new Long(504238974), row.get("value")));
	}

	@Test
	public void testToFloat(){
		testCall(db,"return apoc.number.exact.toFloat('50423.1656', 10, null) as value", row -> assertEquals(new Double(50423.1656), row.get("value")));
	}

	@Test
	public void testToExact(){
		testCall(db,"return apoc.number.exact.toExact(521468545698447) as value", row -> assertEquals(new Long("521468545698447"), row.get("value")));
	}

	@Test
	public void testPrec(){
		testCall(db,"return apoc.number.exact.mul('550058444','662557', 5, 'HALF_DOWN') as value", row -> assertEquals(new String("364450000000000"), row.get("value")));
	}

	@Test
	public void testRound(){
		testCall(db,"return apoc.number.exact.mul('550058444','662557', 10, 'DOWN') as value", row -> assertEquals(new String("364445072400000"), row.get("value")));
	}

	@Test
	public void testMulWithoutOptionalParams(){
		testCall(db,"return apoc.number.exact.mul('550058444','662557') as value", row -> assertEquals(new String("364445072481308"), row.get("value")));
	}

	@Test
	public void testAddScientificNotation(){
		testCall(db,"return apoc.number.exact.add('1E6','1E6') as value", row -> assertEquals(new String("2000000"), row.get("value")));
	}

}
