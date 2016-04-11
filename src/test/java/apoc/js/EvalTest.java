package apoc.js;


import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestGraphDatabaseFactory;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.*;


public class EvalTest {
	private GraphDatabaseService db;
	public @Rule ExpectedException expected = ExpectedException.none();

	@Before
	public void sUp() throws Exception {
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		TestUtil.registerProcedure(db, Eval.class);
	}

	@After
	public void tearDown() {
		db.shutdown();
	}

	@Test
	public void shouldEvalBasicExpressions() throws Exception {	// basic == context-independent
		testCall(db,
				"CALL apoc.js.eval('1 + 1') YIELD value as result",
				row -> assertEquals(2, row.get("result"))
		);

		testCall(db,
				"CALL apoc.js.eval('var z = 0; var o = 1; o / z;') YIELD value as result",
				row -> assertEquals(Double.POSITIVE_INFINITY, row.get("result"))
		);
	}

	@Test(expected = QueryExecutionException.class)
	public void shouldCaptureJsExceptions() throws Exception {	// basic == context-independent
		testCall(db,
				"CALL apoc.js.eval('return 0') YIELD value as result",
				row -> assertEquals(2, row.get("result"))
		);
	}

	@Test
	public void shouldWorkWithParams() throws Exception {	// basic == context-independent
		testCall(db,
				"CALL apoc.js.evalWithParams('var v = x; x', { x : 1 }) YIELD value as result",
				row -> assertEquals(1L, row.get("result"))
		);

		String expr =
				"String.prototype.capitalize = function() {\n" +
				"    return this.charAt(0).toUpperCase() + this.slice(1);\n" +
				"}; \n" +
				"var fullName = ln.capitalize() + \\', \\' + fn.capitalize();\n" +
				"fullName";
		testCall(db,
				"CALL apoc.js.evalWithParams('" + expr + "', { fn: 'john', ln: 'doe' }) YIELD value as result",
				row -> assertEquals("Doe, John", row.get("result"))
		);
	}

	@Test
	public void shouldNotClogGlobalContext() throws Exception {	// basic == context-independent

		// given
		String expr =
				"String.prototype.capitalize = function() {\n" +
						"    return this.charAt(0).toUpperCase() + this.slice(1);\n" +
						"}; \n" +
						"var fullName = ln.capitalize() + \\', \\' + fn.capitalize();\n" +
						"fullName";

		// when
		testCall(db,
				"CALL apoc.js.evalWithParams('" + expr + "', { fn: 'john', ln: 'doe' }) YIELD value as result",
				row -> assertEquals("Doe, John", row.get("result"))
		);

		// then: subsequent calls shouldn't see prev. calls' bindings, if any.
		expected.expect(QueryExecutionException.class);
		expected.expectMessage("Failed to evaluate fullName");
		testCall(db,
				"CALL apoc.js.eval('fullName') YIELD value as result",
				row -> assertEquals("Doe, John", row.get("result"))
		);

		expected.expect(QueryExecutionException.class);
		expected.expectMessage("Failed to evaluate capitalize");
		testCall(db,
				"CALL apoc.js.eval('\"str\".capitalize()') YIELD value as result",
				row -> assertEquals("Doe, John", row.get("result"))
				);
	}
}
