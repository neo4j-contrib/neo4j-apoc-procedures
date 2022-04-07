package apoc.custom;

import apoc.RegisterComponentFactory;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.StatusCodeMatcher;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.custom.CypherProceduresHandler.FUNCTION;
import static apoc.custom.CypherProceduresHandler.PROCEDURE;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProceduresTest  {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, CypherProcedures.class);
    }

    @Test
    public void registerSimpleStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        db.executeTransactionally("CALL apoc.custom.declareProcedure('answer2() :: (answer::INT)','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.answer2()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void overrideSingleCallStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.answer() yield row return row", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 43 as answer')");
        TestUtil.testCall(db, "call custom.answer() yield row return row", (row) -> assertEquals(43L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void overrideCypherCallStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "with 1 as foo call custom.answer() yield row return row", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 43 as answer')");
        TestUtil.testCall(db, "with 1 as foo call custom.answer() yield row return row", (row) -> assertEquals(43L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResults() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer','read',[['answer','long']])");
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $answer as answer')");
        TestUtil.testCall(db, "call custom.answer({answer:42})", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',null,[['input','number']])");
        TestUtil.testCall(db, "call custom.answer(42)", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']])");
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypes() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','read',null," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']])");
        TestUtil.testCall(db, "call custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2}))", (row) -> assertEquals(9, ((List)((Map)row.get("row")).get("data")).size()));
    }

    @Test
    public void  testDeclareFunctionReturnTypes() {
        // given
        db.executeTransactionally("UNWIND range(1, 4) as val CREATE (i:Target {value: val});");

        // when
        db.executeTransactionally("CALL apoc.custom.declareFunction('ret_node(val :: INTEGER) :: NODE ', 'MATCH (t:Target {value : $val}) RETURN t')");
        db.executeTransactionally("CALL apoc.custom.declareFunction('ret_node_list(val :: INTEGER) :: LIST OF NODE ', 'MATCH (t:Target {value : $val}) RETURN [t]')");
        db.executeTransactionally("CALL apoc.custom.declareFunction('ret_map(val :: INTEGER) :: MAP ', 'RETURN {value : $val} as value')");
        db.executeTransactionally("CALL apoc.custom.declareFunction('ret_map_list(val :: INTEGER) :: LIST OF MAP ', 'RETURN [{value : $val}] as value')");

        // then
        TestUtil.testResult(db, "RETURN custom.ret_node(1) AS val", (result) -> {
            Node node = result.<Node>columnAs("val").next();
            assertTrue(node.hasLabel(Label.label("Target")));
            assertEquals(1L, node.getProperty("value"));
        });
        TestUtil.testResult(db, "RETURN custom.ret_node_list(2) AS val", (result) -> {
            List<List<Node>> nodes = result.<List<List<Node>>>columnAs("val").next();
            assertEquals(1, nodes.size());
            Node node = nodes.get(0).get(0);
            assertTrue(node.hasLabel(Label.label("Target")));
            assertEquals(2L, node.getProperty("value"));
        });
        TestUtil.testResult(db, "RETURN custom.ret_map(3) AS val", (result) -> {
            Map<String, Map<String, Object>> map = result.<Map<String, Map<String, Object>>>columnAs("val").next();
            assertEquals(1, map.size());
            assertEquals(3L, map.get("value").get("value"));
        });
        TestUtil.testResult(db, "RETURN custom.ret_map_list(4) AS val", (result) -> {
            List<Map<String, List<Map<String, Object>>>> list = result.<List<Map<String, List<Map<String, Object>>>>>columnAs("val").next();
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).size());
            assertEquals(4L, list.get(0).get("value").get(0).get("value"));
        });
    }

    @Test
    public void testRegisterFunctionReturnTypes() {
        // given
        db.executeTransactionally("UNWIND range(1, 4) as val CREATE (i:Target {value: val});");

        // when
        db.executeTransactionally("CALL apoc.custom.asFunction('ret_node', 'MATCH (t:Target {value : $val}) RETURN t', 'NODE', [['val', 'INTEGER']])");
        db.executeTransactionally("CALL apoc.custom.asFunction('ret_node_list', 'MATCH (t:Target {value : $val}) RETURN t', 'LIST OF NODE', [['val', 'INTEGER']])");
        db.executeTransactionally("CALL apoc.custom.asFunction('ret_map', 'MATCH (t:Target {value : $val}) RETURN t', 'MAP', [['val', 'INTEGER']])");
        db.executeTransactionally("CALL apoc.custom.asFunction('ret_map_list', 'MATCH (t:Target {value : $val}) RETURN t', 'LIST OF MAP', [['val', 'INTEGER']])");
        // then
        TestUtil.testResult(db, "RETURN custom.ret_node(1) AS val", (result) -> {
            Node node = result.<Node>columnAs("val").next();
            assertTrue(node.hasLabel(Label.label("Target")));
            assertEquals(1L, node.getProperty("value"));
        });
        TestUtil.testResult(db, "RETURN custom.ret_node_list(2) AS val", (result) -> {
            List<Node> nodes = result.<List<Node>>columnAs("val").next();
            assertEquals(1, nodes.size());
            Node t = nodes.get(0);
            assertTrue(t.hasLabel(Label.label("Target")));
            assertEquals(2L, t.getProperty("value"));
        });
        TestUtil.testResult(db, "RETURN custom.ret_map(3) AS val", (result) -> {
            Map<String,Node> map = result.<Map<String, Node>>columnAs("val").next();
            assertEquals(1, map.size());
            Node t = map.get("t");
            assertTrue(t.hasLabel(Label.label("Target")));
            assertEquals(3L, t.getProperty("value"));
        });
        TestUtil.testResult(db, "RETURN custom.ret_map_list(4) AS val", (result) -> {
            List<Map<String, Node>> nodes = result.<List<Map<String, Node>>>columnAs("val").next();
            assertEquals(1, nodes.size());
            Node t = nodes.get(0).get("t");
            assertTrue(t.hasLabel(Label.label("Target")));
            assertEquals(4L, t.getProperty("value"));
        });
    }

    @Test
    public void testStatementReturningNode() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','create path=(node)-[relationship:FOO]->() return node, relationship, path','write', [['node','Node'], ['relationship','RELATIONSHIP'], ['path','PATH']], [])");
        TestUtil.testCall(db, "call custom.answer()", (row) -> {});
    }

    @Test
    public void registerSimpleStatementFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        db.executeTransactionally("CALL apoc.custom.declareFunction('answer2() :: STRING','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.answer2() as row", (row) -> assertEquals(42L, row.get("row")));
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42 as answer','long')");
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunctionUnnamedResultColumn() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatementFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $answer as answer','long')");
        TestUtil.testCall(db, "return custom.answer({answer:42}) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatementFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $input as answer','long',[['input','number']])");
        TestUtil.testCall(db, "return custom.answer(42) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypesFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','list of any'," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']], true)");
        TestUtil.testCall(db, "return custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2})) as data", (row) -> assertEquals(9, ((List)row.get("data")).size()));
    }

    @Test
    public void shouldRegisterSimpleStatementWithDescription() throws Exception {
        // given
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer', 'read', null, null, 'Answer to the Ultimate Question of Life, the Universe, and Everything')");

        // when
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));

        // then
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("Answer to the Ultimate Question of Life, the Universe, and Everything", row.get("description"));
            assertEquals("procedure", row.get("type"));
        });
    }

    @Test
    public void shouldRegisterSimpleStatementFunctionDescription() throws Exception {
        // given
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42 as answer', '', null, false, 'Answer to the Ultimate Question of Life, the Universe, and Everything')");

        // when
        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));

        // then
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("Answer to the Ultimate Question of Life, the Universe, and Everything", row.get("description"));
            assertEquals("function", row.get("type"));
        });
    }

    @Test
    public void shouldListAllProceduresAndFunctions() throws Exception {
        // given
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']], 'Procedure that answer to the Ultimate Question of Life, the Universe, and Everything')");
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $input as answer','long', [['input','number']], false)");
        // System.out.println(db.execute("call apoc.custom.list").resultAsString());

        // when
        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
            // then
            assertTrue(row.hasNext());
            while (row.hasNext()){
                Map<String, Object> value = row.next();
                assertTrue(value.containsKey("type"));
                assertTrue(FUNCTION.equals(value.get("type")) || PROCEDURE.equals(value.get("type")));

                if(PROCEDURE.equals(value.get("type"))){
                    assertEquals("answer", value.get("name"));
                    assertEquals(asList(asList("answer", "number")), value.get("outputs"));
                    assertEquals(asList(asList("input", "integer", "42")), value.get("inputs"));
                    assertEquals("Procedure that answer to the Ultimate Question of Life, the Universe, and Everything", value.get("description").toString());
                    assertNull(value.get("forceSingle"));
                    assertEquals("read", value.get("mode"));
                }

                if(FUNCTION.equals(value.get("type"))){
                    assertEquals("answer", value.get("name"));
                    assertEquals("integer", value.get("outputs"));
                    assertEquals(asList(asList("input", "number")), value.get("inputs"));
                    assertEquals("", value.get("description"));
                    assertFalse((Boolean) value.get("forceSingle"));
                    assertNull(value.get("mode"));
                }
            }
        });
    }

    @Test
    public void shouldProvideAnEmptyList() throws Exception {
        // when
        TestUtil.testResult(db, "call apoc.custom.list", (row) ->
            // then
            assertFalse(row.hasNext())
        );
    }

    @Test
    public void shouldRemoveTheCustomProcedure() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("There is no procedure with the name `custom.answer` registered for this database instance. " +
                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.");
        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));

        // given
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));

        // when
        db.executeTransactionally("call apoc.custom.removeProcedure('answer')");
        db.executeTransactionally("call db.clearQueryCaches()");

        // then
        TestUtil.count(db, "call custom.answer()");
    }

    @Test
    public void shouldOverrideAndRemoveTheCustomFunctionWithDotInName() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Unknown function 'custom.a.b.c'");
        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));

        // given
        db.executeTransactionally("CALL apoc.custom.asFunction('a.b.c','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.a.b.c() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");
        db.executeTransactionally("CALL apoc.custom.asFunction('a.b.c','RETURN 43 as answer')");
        TestUtil.testCall(db, "return custom.a.b.c() as row", (row) -> assertEquals(43L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        // when
        db.executeTransactionally("call apoc.custom.removeFunction('a.b.c')");
        db.executeTransactionally("call db.clearQueryCaches()");

        // then
        TestUtil.count(db, "RETURN custom.a.b.c()");
    }

    @Test
    public void shouldOverrideAndRemoveTheCustomProcedureWithDotInName() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("There is no procedure with the name `custom.a.b.c` registered for this database instance. " +
                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.");
        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));

        // given
        db.executeTransactionally("call apoc.custom.asProcedure('a.b.c','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.a.b.c()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");
        db.executeTransactionally("call apoc.custom.asProcedure('a.b.c','RETURN 43 as answer')");
        TestUtil.testCall(db, "call custom.a.b.c()", (row) -> assertEquals(43L, ((Map)row.get("row")).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        // when
        db.executeTransactionally("call apoc.custom.removeProcedure('a.b.c')");
        db.executeTransactionally("call db.clearQueryCaches()");

        // then
        TestUtil.count(db, "call custom.a.b.c()");
    }

    @Test
    public void shouldRemoveTheCustomFunctionWithDotInName() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Unknown function 'custom.a.b.c'");
        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));

        // given
        db.executeTransactionally("CALL apoc.custom.asFunction('a.b.c','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.a.b.c() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));

        // when
        db.executeTransactionally("call apoc.custom.removeFunction('a.b.c')");
        db.executeTransactionally("call db.clearQueryCaches()");

        // then
        TestUtil.count(db, "RETURN custom.a.b.c()");
    }

    @Test
    public void shouldRemoveTheCustomProcedureWithDotInName() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("There is no procedure with the name `custom.a.b.c` registered for this database instance. " +
                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.");
        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));

        // given
        db.executeTransactionally("call apoc.custom.asProcedure('a.b.c','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.a.b.c()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));

        // when
        db.executeTransactionally("call apoc.custom.removeProcedure('a.b.c')");
        db.executeTransactionally("call db.clearQueryCaches()");

        // then
        TestUtil.count(db, "call custom.a.b.c()");
    }

    @Test
    public void shouldOverrideCustomFunctionWithDotInNameOnlyIfWithSameNamespaceAndFinalName() throws Exception {

        // given
        db.executeTransactionally("call apoc.custom.asFunction('a.b.name','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.a.b.name() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        db.executeTransactionally("call apoc.custom.asFunction('a.b.name','RETURN 34 as answer')");
        TestUtil.testCall(db, "return custom.a.b.name() as row", (row) -> assertEquals(34L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        db.executeTransactionally("call apoc.custom.asFunction('x.z.name','RETURN 12 as answer')");
        TestUtil.testCall(db, "return custom.x.z.name() as row", (row) -> assertEquals(12L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "return custom.a.b.name() as row", (row) -> assertEquals(34L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
            assertTrue(row.hasNext());
            Map<String, Object> mapFirst = row.next();
            assertEquals("a.b.name", mapFirst.get("name"));
            assertEquals("RETURN 34 as answer", mapFirst.get("statement"));
            assertEquals(FUNCTION, mapFirst.get("type"));
            assertTrue(row.hasNext());
            Map<String, Object> mapSecond = row.next();
            assertEquals("x.z.name", mapSecond.get("name"));
            assertEquals("RETURN 12 as answer", mapSecond.get("statement"));
            assertEquals(FUNCTION, mapSecond.get("type"));
            assertFalse(row.hasNext());
        });

        db.executeTransactionally("call apoc.custom.removeFunction('a.b.name')");
        db.executeTransactionally("call db.clearQueryCaches()");

        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
            assertTrue(row.hasNext());
            Map<String, Object> map = row.next();
            assertEquals("x.z.name", map.get("name"));
            assertEquals("RETURN 12 as answer", map.get("statement"));
            assertEquals(FUNCTION, map.get("type"));
            assertFalse(row.hasNext());
        });

        db.executeTransactionally("call apoc.custom.removeFunction('x.z.name')");
        db.executeTransactionally("call db.clearQueryCaches()");

        TestUtil.testCallEmpty(db, "call apoc.custom.list()", Collections.emptyMap());
    }

    @Test
    public void shouldOverrideCustomProcedureWithDotInNameOnlyIfWithSameNamespaceAndFinalName() throws Exception {

        // given
        db.executeTransactionally("call apoc.custom.asProcedure('a.b.name','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.a.b.name()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        db.executeTransactionally("call apoc.custom.asProcedure('a.b.name','RETURN 34 as answer')");
        TestUtil.testCall(db, "call custom.a.b.name()", (row) -> assertEquals(34L, ((Map)row.get("row")).get("answer")));
        db.executeTransactionally("call db.clearQueryCaches()");

        db.executeTransactionally("call apoc.custom.asProcedure('x.z.name','RETURN 12 as answer')");
        TestUtil.testCall(db, "call custom.x.z.name()", (row) -> assertEquals(12L, ((Map)row.get("row")).get("answer")));
        TestUtil.testCall(db, "call custom.a.b.name()", (row) -> assertEquals(34L, ((Map)row.get("row")).get("answer")));

        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
            assertTrue(row.hasNext());
            Map<String, Object> mapFirst = row.next();
            assertEquals("a.b.name", mapFirst.get("name"));
            assertEquals("RETURN 34 as answer", mapFirst.get("statement"));
            assertEquals(PROCEDURE, mapFirst.get("type"));
            assertTrue(row.hasNext());
            Map<String, Object> mapSecond = row.next();
            assertEquals("x.z.name", mapSecond.get("name"));
            assertEquals("RETURN 12 as answer", mapSecond.get("statement"));
            assertEquals(PROCEDURE, mapSecond.get("type"));
            assertFalse(row.hasNext());
        });

        db.executeTransactionally("call apoc.custom.removeProcedure('a.b.name')");
        db.executeTransactionally("call db.clearQueryCaches()");

        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
            assertTrue(row.hasNext());
            Map<String, Object> map = row.next();
            assertEquals("x.z.name", map.get("name"));
            assertEquals("RETURN 12 as answer", map.get("statement"));
            assertEquals(PROCEDURE, map.get("type"));
            assertFalse(row.hasNext());
        });

        db.executeTransactionally("call apoc.custom.removeProcedure('x.z.name')");
        db.executeTransactionally("call db.clearQueryCaches()");

        TestUtil.testCallEmpty(db, "call apoc.custom.list()", Collections.emptyMap());
    }

    @Test
    public void shouldRemoveTheCustomFunction() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Unknown function 'custom.answer'");
        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));

        // given
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));

        // when
        db.executeTransactionally("call apoc.custom.removeFunction('answer')");
        db.executeTransactionally("call db.clearQueryCaches()");

        // then
        TestUtil.count(db, "return custom.answer()");
    }

    @Test
    public void shouldOverwriteAndRemoveCustomProcedure() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42')");
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42')");
        assertEquals("Expecting one procedure listed", 1, TestUtil.count(db, "call apoc.custom.list()"));
        db.executeTransactionally("call apoc.custom.removeProcedure('answer')");
    }

    @Test
    public void shouldOverwriteAndRemoveCustomFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
        assertEquals("Expecting one function listed", 1, TestUtil.count(db, "call apoc.custom.list()"));
        db.executeTransactionally("call apoc.custom.removeFunction('answer')");
    }

    @Test
    public void shouldRemovalOfProcedureNodeDeactivate() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("There is no procedure with the name `custom.answer` registered for this database instance. " +
                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.");
        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));

        //given
        db.executeTransactionally("call apoc.custom.asProcedure('answer', 'RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));

        // remove the node in systemdb
        GraphDatabaseService systemDb = db.getManagementService().database("system");
        try (Transaction tx = systemDb.beginTx()) {
            Node node = tx.findNode(SystemLabels.ApocCypherProcedures, SystemPropertyKeys.name.name(), "answer");
            node.delete();
            tx.commit();
        }

        // refresh procedures
        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle = db.getDependencyResolver().resolveDependency(RegisterComponentFactory.RegisterComponentLifecycle.class);
        CypherProceduresHandler cypherProceduresHandler = (CypherProceduresHandler) registerComponentLifecycle.getResolvers().get(CypherProceduresHandler.class).get(db.databaseName());
        cypherProceduresHandler.restoreProceduresAndFunctions();

        // when
        TestUtil.count(db, "call custom.answer()");
    }

    @Test
    public void shouldRemovalOfFunctionNodeDeactivate() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Unknown function 'custom.answer'");
        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));

        //given
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");

        long answer = TestUtil.singleResultFirstColumn(db, "return custom.answer()");
        assertEquals(42L, answer);

        // remove the node in systemdb
        GraphDatabaseService systemDb = db.getManagementService().database("system");
        try (Transaction tx = systemDb.beginTx()) {
            Node node = tx.findNode(SystemLabels.ApocCypherProcedures, SystemPropertyKeys.name.name(), "answer");
            node.delete();
            tx.commit();
        }

        // refresh procedures
        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle = db.getDependencyResolver().resolveDependency(RegisterComponentFactory.RegisterComponentLifecycle.class);
        CypherProceduresHandler cypherProceduresHandler = (CypherProceduresHandler) registerComponentLifecycle.getResolvers().get(CypherProceduresHandler.class).get(db.databaseName());
        cypherProceduresHandler.restoreProceduresAndFunctions();

        // when
        TestUtil.singleResultFirstColumn(db, "return custom.answer()");
    }
}
