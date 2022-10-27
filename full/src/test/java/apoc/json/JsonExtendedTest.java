package apoc.json;

import apoc.convert.Json;
import apoc.cypher.Cypher;
import apoc.load.LoadJson;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;

import static apoc.load.util.ConversionUtil.ERR_PREFIX;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.*;


public class JsonExtendedTest {

    private static final String INVALID_JSON = "{\"foo\":[\"1\", {\n" +
            "    \"bar\": 18446744062065078016838\n" +
            "  }, {\"baz\":  1228446744062065078016838}],\n" +
            "    \"baz\": 7 \n" +
            "}";

    private static final String VALID_JSON = "{\"foo\":[\"1\", {\n" +
            "    \"bar\": 123\n" +
            "  }, {\"baz\":  456}],\n" +
            "    \"baz\": 7 \n" +
            "}";
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() throws Exception {
        TestUtil.registerProcedure(db, JsonExtended.class, Json.class, LoadJson.class, Cypher.class);
    }

    @Test
    public void testJsonValidate() {
        testResult(db, "CALL apoc.json.validate('{\"osvaldo\": [1,2,3],\n" +
                        "  \"foo\":[\"1\", {\n" +
                        "    \"bar\": 28446744062065078016838\n" +
                        "  }, {\"baz\":  18446744062065078016838}],\n" +
                        "    \"baz\": 7 \n" +
                        "}', '$')",
                (r) -> {
                    final List<String> value = Iterators.asList(r.columnAs("value"));
                    assertEquals(2, value.size());
                    assertTrue(value.stream().allMatch(err ->
                            (err.startsWith(ERR_PREFIX + "baz") || err.startsWith(ERR_PREFIX + "bar"))
                                    && err.contains("out of range of long ")));
                });
    }

    @Test
    public void testFailOnErrorGetJsonProperty() {
        db.executeTransactionally("CREATE (n:JsonInvalidNode {prop: '{\"foo\": [{\"baz\": 18446744062065078016838}],\"baz\": 7}'})");
        db.executeTransactionally("CREATE (n:JsonValidNode {prop: '{\"foo\": [{\"baz\": 123}],\"baz\": 7}'})");
        
        failJsonPropCommon("apoc.convert.getJsonPropertyMap(n, 'prop', null, [])");
        failJsonPropCommon("apoc.convert.getJsonPropertyMap(n, 'prop', '$', [])");
        failJsonPropCommon("apoc.convert.getJsonProperty(n, 'prop', '$..baz', [])");
    }
    
    @Test
    public void testFailOnError() {
        failJsonStringCommon("apoc.convert.fromJsonMap($json, null)");
        failJsonStringCommon("apoc.convert.fromJsonMap($json, '$')");
        failJsonStringCommon("apoc.json.path($json, '$..baz')");
        failJsonStringCommon("apoc.convert.fromJsonList($json, '$..bar')");
    }

    private void failJsonPropCommon(String query) {
        final String call = "CALL { WITH n CALL apoc.json.validate(n.prop) YIELD value RETURN collect(value) AS errs }" +
                "RETURN CASE errs WHEN [] THEN " + query + " ELSE 0 END AS value";
        
        failJsonCommon("MATCH (n:JsonInvalidNode) RETURN " + query,
                "MATCH (n:JsonInvalidNode) WITH n " + call,
                "MATCH (n:JsonValidNode) RETURN " + query,
                "MATCH (n:JsonValidNode)  WITH n " + call
        );
    }

    private void failJsonStringCommon(String query) {
        final String query1 = "RETURN " + query;
        final String call = "CALL apoc.json.validate($json) YIELD value " +
                "RETURN CASE collect(value) WHEN [] THEN " + query + " ELSE 0 END AS value";
        failJsonCommon(query1, call, query1, call);
    }

    private void failJsonCommon(String call, String call1, String call2, String call3) {

        // should throw an "out of range" error
//        final String call = "RETURN " + query;
        try {
            testCall(db, call,
                    map("json", INVALID_JSON),
                    (row) -> fail("Should throw an RuntimeException with message 'out of range'"));
        } catch (RuntimeException e) {
            final String message = e.getMessage();
            assertTrue("The actual err. message is: " + message, message.contains("out of range"));
        }

        // should return an empty list with apoc.json.validate
//        final String call1 = "CALL apoc.json.validate($json) YIELD value " +
//                "RETURN CASE collect(value) WHEN [] THEN " + query + " ELSE [] END AS value";
        testCall(db, call1,
                map("json", INVALID_JSON),
                (row) -> assertEquals(0L, row.get("value")));

        // valid json
        final Object json = TestUtil.singleResultFirstColumn(db, call2,
                map("json", VALID_JSON));

        // should return an empty list with apoc.json.validate
        testCall(db, call3,
                map("json", VALID_JSON),
                (row) -> {
                    final Object value = row.get("value");
                    assertEquals(json, value);
                    assertNotEquals(emptyList(), value);
                    assertNotEquals(emptyMap(), value);
                });
    }
    
    /*
    with [1] as list 
call apoc.do.when(list = [], "return apoc.json.path('{}', '') as value", "return 0") 
yield value return value.value
     */
}
