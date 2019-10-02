package apoc.convert;

import apoc.util.TestUtil;
import junit.framework.TestCase;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class ConvertJsonTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

	@Before public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Json.class);
    }

    @Test public void testToJsonList() throws Exception {
	    testCall(db, "RETURN apoc.convert.toJson([1,2,3]) as value",
	             (row) -> assertEquals("[1,2,3]", row.get("value")) );
    }
    @Test public void testToJsonMap() throws Exception {
	    testCall(db, "RETURN apoc.convert.toJson({a:42,b:\"foo\",c:[1,2,3]}) as value",
	             (row) -> assertEquals("{\"a\":42,\"b\":\"foo\",\"c\":[1,2,3]}", row.get("value")) );
    }
    @Test public void testFromJsonList() throws Exception {
	    testCall(db, "RETURN apoc.convert.fromJsonList('[1,2,3]') as value",
	             (row) -> assertEquals(asList(1L,2L,3L), row.get("value")) );
	    testCall(db, "RETURN apoc.convert.fromJsonList('{\"foo\":[1,2,3]}','$.foo') as value",
	             (row) -> assertEquals(asList(1L,2L,3L), row.get("value")) );
    }
    @Test public void testFromJsonMap() throws Exception {
	    testCall(db, "RETURN apoc.convert.fromJsonMap('{\"a\":42,\"b\":\"foo\",\"c\":[1,2,3]}')  as value",
	             (row) -> {
		           Map value = (Map)row.get("value");
		           assertEquals(42L, value.get("a"));
		           assertEquals("foo", value.get("b"));
		           assertEquals(asList(1L,2L,3L), value.get("c"));
		         });
    }

    @Test public void testSetJsonProperty() throws Exception {
        testCall(db, "CREATE (n) WITH n CALL apoc.convert.setJsonProperty(n, 'json', [1,2,3]) RETURN n",
                (row) -> assertEquals("[1,2,3]", ((Node)row.get("n")).getProperty("json")));
    }

    @Test public void testGetJsonProperty() throws Exception {
        testCall(db, "CREATE (n {json:'[1,2,3]'}) RETURN apoc.convert.getJsonProperty(n, 'json') AS value",
                (row) -> assertEquals(asList(1L,2L,3L), row.get("value")) );
        testCall(db, "CREATE (n {json:'{\"foo\":[1,2,3]}'}) RETURN apoc.convert.getJsonProperty(n, 'json','$.foo') AS value",
                (row) -> assertEquals(asList(1L,2L,3L), row.get("value")) );
    }
    @Test public void testGetJsonPropertyMap() throws Exception {
        testCall(db, "CREATE (n {json:'{a:[1,2,3]}'}) RETURN apoc.convert.getJsonProperty(n, 'json') as value",
                (row) -> assertEquals(map("a",asList(1L,2L,3L)), row.get("value")) );
        testCall(db, "CREATE (n {json:'{a:[1,2,3]}'}) RETURN apoc.convert.getJsonProperty(n, 'json','$.a') as value",
                (row) -> assertEquals(asList(1L,2L,3L), row.get("value")) );
    }

    @Test public void testToTree() throws Exception {
        testCall(db, "CREATE p1=(m:Movie {title:'M'})<-[:ACTED_IN {role:'R1'}]-(:Actor {name:'A1'}), " +
                " p2 = (m)<-[:ACTED_IN  {role:'R2'}]-(:Actor {name:'A2'}) WITH [p1,p2] as paths " +
                " CALL apoc.convert.toTree(paths) YIELD value RETURN value",
                (row) -> {
                    Map root = (Map) row.get("value");
                    assertEquals("Movie", root.get("_type"));
                    assertEquals("M", root.get("title"));
                    List<Map> actors = (List<Map>) root.get("acted_in");
                    assertEquals("Actor", actors.get(0).get("_type"));
                    assertEquals(true, actors.get(0).get("name").toString().matches("A[12]"));
                    assertEquals(true, actors.get(0).get("acted_in.role").toString().matches("R[12]"));
                });
    }
    @Test public void testToTreeUpperCaseRels() throws Exception {
        testCall(db, "CREATE p1=(m:Movie {title:'M'})<-[:ACTED_IN {role:'R1'}]-(:Actor {name:'A1'}), " +
                " p2 = (m)<-[:ACTED_IN  {role:'R2'}]-(:Actor {name:'A2'}) WITH [p1,p2] as paths " +
                " CALL apoc.convert.toTree(paths,false) YIELD value RETURN value",
                (row) -> {
                    Map root = (Map) row.get("value");
                    assertEquals("Movie", root.get("_type"));
                    assertEquals("M", root.get("title"));
                    List<Map> actors = (List<Map>) root.get("ACTED_IN");
                    assertEquals("Actor", actors.get(0).get("_type"));
                    assertEquals(true, actors.get(0).get("name").toString().matches("A[12]"));
                    assertEquals(true, actors.get(0).get("ACTED_IN.role").toString().matches("R[12]"));
                });
    }
    @Test public void testTreeOfEmptyList() throws Exception {
        testCall(db, " CALL apoc.convert.toTree([]) YIELD value RETURN value",
                (row) -> {
                    Map root = (Map) row.get("value");
                    assertTrue(root.isEmpty());
                });
    }

    @Test public void testToTreeLeafNodes() throws Exception {
        String createStatement = "CREATE\n" +
                "  (c1:Category {name: 'PC'}),\n" +
                "    (c1)-[:subcategory {id:1}]->(c2:Category {name: 'Parts'}),\n" +
                "      (c2)-[:subcategory {id:2}]->(c3:Category {name: 'CPU'})";
        db.executeTransactionally(createStatement);

        String call = "MATCH p=(n:Category)-[:subcategory*]->(m)\n" +
                "WHERE NOT (m)-[:subcategory]->() AND NOT ()-[:subcategory]->(n)\n" +
                "WITH COLLECT(p) AS ps\n" +
                "CALL apoc.convert.toTree(ps) yield value\n" +
                "RETURN value;";
        testCall(db, call,
                (row) -> {
                    Map root = (Map) row.get("value");

                    assertEquals("Category", root.get("_type"));
                    assertEquals("PC", root.get("name"));
                    List<Map> parts = (List<Map>) root.get("subcategory");
                    assertEquals(1,parts.size());
                    Map pcParts = parts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertEquals("Parts", pcParts.get("name"));
                    List<Map> subParts = (List<Map>)pcParts.get("subcategory");
                    Map cpu = subParts.get(0);
                    assertEquals(1,subParts.size());
                    assertEquals("Category", cpu.get("_type"));
                    assertEquals("CPU", cpu.get("name"));
                });
    }
    @Test public void testToJsonMapSortingProperties() throws Exception {
        testCall(db, "WITH {b:8, d:3, a:2, E: 12, C:9} as map RETURN apoc.convert.toSortedJsonMap(map, false) as value",
                (row) -> assertEquals("{\"C\":9,\"E\":12,\"a\":2,\"b\":8,\"d\":3}", row.get("value")) );
    }

    @Test public void testToJsonMapSortingPropertiesIgnoringCase() throws Exception {
        testCall(db, "WITH {b:8, d:3, a:2, E: 12, C:9} as map RETURN apoc.convert.toSortedJsonMap(map) as value",
                (row) -> assertEquals("{\"a\":2,\"b\":8,\"C\":9,\"d\":3,\"E\":12}", row.get("value")) );
    }

    @Test
    public void testToTreeParentNodes () {
	    String createDatabase = "CREATE (b:Bib {id: '57523a6f-fda9-4a61-c4f6-08d47cdcf0cd', langId: 2})-[:HAS]->(c:Comm {id: 'a34fd608-1751-0b5d-cb38-6991297fa9c9', langId: 2}), \n" +
                "(b)-[:HAS]->(c1:Comm {id: 'a34fd608-262b-678a-cb38-6991297fa9c8', langId: 2}),\n" +
                "(u:User {id: 'facebook|680594762097202'})-[:Flag {Created: '2018-11-21T11:22:01', FlagType: 4}]->(c1),\n" +
                "(u)-[:Flag {Created: '2018-11-21T11:22:04', FlagType: 5}]->(c),\n" +
                "(u1:User {id: 'google-oauth2|106707535753175966005'})-[:Flag {Created: '2018-11-21T11:20:34', FlagType: 2}]->(c),\n" +
                "(u1)-[:Flag {Created: '2018-11-21T11:20:31', FlagType: 1}]->(c1)";
        db.executeTransactionally(createDatabase);

        String call = "MATCH (parent:Bib {id: '57523a6f-fda9-4a61-c4f6-08d47cdcf0cd'})\n" +
                "WITH parent\n" +
                "OPTIONAL MATCH childFlagPath=(parent)-[:HAS]->(:Comm)<-[:Flag]-(:User)\n" +
                "WITH COLLECT(childFlagPath) AS cfp\n" +
                "CALL apoc.convert.toTree(cfp) yield value\n" +
                "RETURN value";

        testCall(db, call,
                (row) -> {
                    Map root = (Map) row.get("value");

                    assertEquals("Bib", root.get("_type"));
                    assertEquals(0L, root.get("_id"));
                    assertEquals("57523a6f-fda9-4a61-c4f6-08d47cdcf0cd", root.get("id"));
                    assertEquals(2L, root.get("langId"));

                    List<Map> has = (List<Map>) root.get("has"); //HAS REL
                    assertEquals(2,has.size());

                    Map hasPart = has.get(0);

                    assertEquals("Comm", hasPart.get("_type"));
                    assertEquals(2L, hasPart.get("_id"));
                    assertEquals("a34fd608-262b-678a-cb38-6991297fa9c8", hasPart.get("id"));
                    assertEquals(2L, hasPart.get("langId"));
                    List<Map> subParts = (List<Map>)hasPart.get("flag");
                    assertEquals(2,subParts.size());
                    Map<String, Object> mapFlag = subParts.get(0);

                    /*USER*/

                    assertEquals("User", mapFlag.get("_type"));
                    assertEquals("2018-11-21T11:20:31",  mapFlag.get("flag.Created"));
                    assertEquals(4L,  mapFlag.get("_id"));
                    assertEquals("google-oauth2|106707535753175966005",  mapFlag.get("id"));
                    assertEquals(1L,  mapFlag.get("flag.FlagType"));

                    mapFlag = subParts.get(1);

                    assertEquals("User", mapFlag.get("_type"));
                    assertEquals("2018-11-21T11:22:01",  mapFlag.get("flag.Created"));
                    assertEquals(3L,  mapFlag.get("_id"));
                    assertEquals("facebook|680594762097202",  mapFlag.get("id"));
                    assertEquals(4L,  mapFlag.get("flag.FlagType"));

                    /*USER*/


                    hasPart = has.get(1);

                    assertEquals("Comm", hasPart.get("_type"));
                    assertEquals(1L, hasPart.get("_id"));
                    assertEquals("a34fd608-1751-0b5d-cb38-6991297fa9c9", hasPart.get("id"));
                    assertEquals(2L, hasPart.get("langId"));
                    subParts = (List<Map>)hasPart.get("flag");
                    assertEquals(2,subParts.size());
                    mapFlag = subParts.get(0);

                    /*USER*/

                    assertEquals("User", mapFlag.get("_type"));
                    assertEquals("2018-11-21T11:20:34",  mapFlag.get("flag.Created"));
                    assertEquals(4L,  mapFlag.get("_id"));
                    assertEquals("google-oauth2|106707535753175966005",  mapFlag.get("id"));
                    assertEquals(2L,  mapFlag.get("flag.FlagType"));

                    mapFlag = subParts.get(1);

                    assertEquals("User", mapFlag.get("_type"));
                    assertEquals("2018-11-21T11:22:04",  mapFlag.get("flag.Created"));
                    assertEquals(3L,  mapFlag.get("_id"));
                    assertEquals("facebook|680594762097202",  mapFlag.get("id"));
                    assertEquals(5L,  mapFlag.get("flag.FlagType"));

                    /*USER*/

                });
    }

    @Test
    public void testToTreeLeafNodesWithConfigInclude() {
        statementForConfig(db);
        String call = "MATCH p=(n:Category)-[:subcategory*]->(m)\n" +
                "WHERE NOT (m)-[:subcategory]->() AND NOT ()-[:subcategory]->(n)\n" +
                "WITH COLLECT(p) AS ps\n" +
                "CALL apoc.convert.toTree(ps, true, {nodes: {Category: ['name']}, rels: {subcategory:['id']}}) yield value\n" +
                "RETURN value;";
        testCall(db, call,
                (row) -> {
                    Map root = (Map) row.get("value");
                    assertEquals("Category", root.get("_type"));
                    assertEquals("PC", root.get("name"));
                    assertNull(root.get("surname"));
                    List<Map> parts = (List<Map>) root.get("subcategory");
                    assertEquals(1,parts.size());
                    Map pcParts = parts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertEquals("Parts", pcParts.get("name"));
                    assertEquals(1L, pcParts.get("subcategory.id"));
                    assertNull(pcParts.get("subcategory.subCat"));
                    List<Map> subParts = (List<Map>)pcParts.get("subcategory");
                    Map cpu = subParts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertEquals("CPU", cpu.get("name"));
                    assertEquals(2L, cpu.get("subcategory.id"));
                    assertNull(cpu.get("subcategory.subCat"));
                });
    }

    @Test
    public void testToTreeLeafNodesWithConfigExclude() {
        statementForConfig(db);
        String call = "MATCH p=(n:Category)-[:subcategory*]->(m)\n" +
                "WHERE NOT (m)-[:subcategory]->() AND NOT ()-[:subcategory]->(n)\n" +
                "WITH COLLECT(p) AS ps\n" +
                "CALL apoc.convert.toTree(ps, true,{nodes: {Category: ['-name']}, rels: {subcategory:['-id']}}) yield value\n" +
                "RETURN value;";
        testCall(db, call,
                (row) -> {
                    Map root = (Map) row.get("value");
                    assertEquals("Category", root.get("_type"));
                    assertFalse("Should not contain key `name`",root.containsKey("name"));
                    assertEquals("computer",root.get("surname"));
                    List<Map> parts = (List<Map>) root.get("subcategory");
                    assertEquals(1,parts.size());
                    Map pcParts = parts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertFalse("Should not contain key `name`",pcParts.containsKey("name"));
                    assertFalse("Should not contain key `subcategory.id`",pcParts.containsKey("subcategory.id"));
                    assertEquals("gen", pcParts.get("subcategory.subCat"));
                    List<Map> subParts = (List<Map>)pcParts.get("subcategory");
                    Map cpu = subParts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertFalse("Should not contain key `name`",cpu.containsKey("name"));
                    assertFalse("Should not contain key `subcategory.id`",cpu.containsKey("subcategory.id"));
                    assertEquals("ex",cpu.get("subcategory.subCat"));
                });
    }

    @Test
    public void testToTreeLeafNodesWithConfigExcludeInclude() {
        statementForConfig(db);
        String call = "MATCH p=(n:Category)-[:subcategory*]->(m)\n" +
                "WHERE NOT (m)-[:subcategory]->() AND NOT ()-[:subcategory]->(n)\n" +
                "WITH COLLECT(p) AS ps\n" +
                "CALL apoc.convert.toTree(ps, true, {nodes: {Category: ['name']}, rels: {subcategory:['-id']}}) yield value\n" +
                "RETURN value;";
        testCall(db, call,
                (row) -> {
                    Map root = (Map) row.get("value");
                    assertEquals("Category", root.get("_type"));
                    assertEquals("PC",root.get("name"));
                    assertFalse("Should not contain key `surname`",root.containsKey("surname"));
                    List<Map> parts = (List<Map>) root.get("subcategory");
                    assertEquals(1,parts.size());
                    Map pcParts = parts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertEquals("Parts", pcParts.get("name"));
                    assertFalse("Should not contain key `subcategory.id`",pcParts.containsKey("subcategory.id"));
                    assertEquals("gen", pcParts.get("subcategory.subCat"));
                    List<Map> subParts = (List<Map>)pcParts.get("subcategory");
                    Map cpu = subParts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertEquals("CPU", cpu.get("name"));
                    assertFalse("Should not contain key `subcategory.id`",cpu.containsKey("subcategory.id"));
                    assertEquals("ex",cpu.get("subcategory.subCat"));
                });
    }

    @Test
    public void testToTreeLeafNodesWithConfigOnlyInclude() {
        statementForConfig(db);
        String call = "MATCH p=(n:Category)-[:subcategory*]->(m)\n" +
                "WHERE NOT (m)-[:subcategory]->() AND NOT ()-[:subcategory]->(n)\n" +
                "WITH COLLECT(p) AS ps\n" +
                "CALL apoc.convert.toTree(ps, true, {nodes: {Category: ['name', 'surname']}}) yield value\n" +
                "RETURN value;";
        testCall(db, call,
                (row) -> {
                    Map root = (Map) row.get("value");
                    assertEquals("Category", root.get("_type"));
                    assertEquals("PC",root.get("name"));
                    assertEquals("computer",root.get("surname"));
                    List<Map> parts = (List<Map>) root.get("subcategory");
                    assertEquals(1,parts.size());
                    Map pcParts = parts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertEquals("Parts", pcParts.get("name"));
                    assertEquals(1L, pcParts.get("subcategory.id"));
                    assertEquals("gen", pcParts.get("subcategory.subCat"));
                    List<Map> subParts = (List<Map>)pcParts.get("subcategory");
                    Map cpu = subParts.get(0);
                    assertEquals("Category", pcParts.get("_type"));
                    assertEquals("CPU", cpu.get("name"));
                    assertEquals(2L, cpu.get("subcategory.id"));
                    assertEquals("ex",cpu.get("subcategory.subCat"));
                });
    }

    @Test
    public void testToTreeLeafNodesWithConfigErrorInclude() {
        statementForConfig(db);
        String call = "MATCH p=(n:Category)-[:subcategory*]->(m)\n" +
                "WHERE NOT (m)-[:subcategory]->() AND NOT ()-[:subcategory]->(n)\n" +
                "WITH COLLECT(p) AS ps\n" +
                "CALL apoc.convert.toTree(ps, true, {nodes: {Category: ['-name','name']}, rels: {subcategory:['-id']}}) yield value\n" +
                "RETURN value;";
        try {
            testResult(db, call, (row) -> { });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals("Only include or exclude attribute are possible!", except.getMessage());
        }

    }

    @Test
    public void testToTreeLeafNodesWithConfigErrorExclude() {
        statementForConfig(db);
        String call = "MATCH p=(n:Category)-[:subcategory*]->(m)\n" +
                "WHERE NOT (m)-[:subcategory]->() AND NOT ()-[:subcategory]->(n)\n" +
                "WITH COLLECT(p) AS ps\n" +
                "CALL apoc.convert.toTree(ps, true, {nodes: {Category: ['-name']}, rels: {subcategory:['-id','name']}}) yield value\n" +
                "RETURN value;";
        try {
            testResult(db, call, (row) -> { });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals("Only include or exclude attribute are possible!", except.getMessage());
        }

    }

    private static void statementForConfig(GraphDatabaseService db) {
        String createStatement = "CREATE\n" +
                "  (c1:Category {name: 'PC', surname: 'computer'}),\n" +
                "    (c1)-[:subcategory {id:1, subCat: 'gen'}]->(c2:Category {name: 'Parts'}),\n" +
                "      (c2)-[:subcategory {id:2, subCat: 'ex'}]->(c3:Category {name: 'CPU'})";

        db.executeTransactionally(createStatement);
    }
}
