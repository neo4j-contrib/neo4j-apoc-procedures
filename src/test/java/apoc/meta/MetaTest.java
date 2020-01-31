package apoc.meta;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.values.storable.*;

import java.time.Clock;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.driver.v1.Values.isoDuration;
import static org.neo4j.graphdb.traversal.Evaluators.toDepth;

public class MetaTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder().newGraphDatabase();
        TestUtil.registerProcedure(db, Meta.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    /*
        @Test public void testMetaStats() throws Exception {
            testResult(db,"CALL apoc.meta.stats", (r) -> assertEquals(true, r.hasNext()));
        }
    */

    @Test
    public void testMetaGraphExtraRels() throws Exception {
        db.execute("CREATE (a:S1 {SomeName1:'aaa'})\n" +
                "CREATE (b:S2 {SomeName2:'bbb'})\n" +
                "CREATE (c:S3 {SomeName3:'ccc'})\n" +
                "CREATE (a)-[:HAS]->(b)\n" +
                "CREATE (b)-[:HAS]->(c)").close();

        testCall(db, "call apoc.meta.graph()",(row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> relationships = (List<Relationship>) row.get("relationships");
            assertEquals(3,nodes.size());
            assertEquals(2,relationships.size());
        });
    }

    @Test
    public void testMetaType() throws Exception {
        try (Transaction tx = db.beginTx()) {
            Node node = db.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("FOO"));
            testTypeName(node, "NODE");
            testTypeName(rel, "RELATIONSHIP");
            Path path = db.traversalDescription().evaluator(toDepth(1)).traverse(node).iterator().next();
// TODO PATH FAILS              testTypeName(path, "PATH");
            tx.failure();
        }
        testTypeName(singletonMap("a", 10), "MAP");
        testTypeName(asList(1, 2), "LIST");
        testTypeName(1L, "INTEGER");
        testTypeName(1, "INTEGER");
        testTypeName(1.0D, "FLOAT");
        testTypeName(1.0, "FLOAT");
        testTypeName("a", "STRING");
        testTypeName(false, "BOOLEAN");
        testTypeName(true, "BOOLEAN");
        testTypeName(null, "NULL");
    }

    @Test
    public void testMetaTypeArray() throws Exception {
        testTypeName(asList(1,2), "LIST");
        testTypeName(asList(LocalDate.of(2018, 1, 1),2), "LIST");
        testTypeName(new Integer[] {1, 2}, "int[]");
        testTypeName(new Float[] {1f, 2f}, "float[]");
        testTypeName(new Double[] {1d, 2d}, "double[]");
        testTypeName(new String[] {"a", "b"}, "String[]");
        testTypeName(new Long[] {1l, 2l}, "long[]");
        testTypeName(new LocalDate[] {LocalDate.of(2018, 1, 1), LocalDate.of(2018, 1, 1)}, "LIST");
        testTypeName(new Object[] {1d, ""}, "LIST");
    }

    @Test
    public void testMetaIsType() throws Exception {
        try (Transaction tx = db.beginTx()) {
            Node node = db.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("FOO"));
            testIsTypeName(node, "NODE");
            testIsTypeName(rel, "RELATIONSHIP");
            Path path = db.traversalDescription().evaluator(toDepth(1)).traverse(node).iterator().next();
// TODO PATH FAILS            testIsTypeName(path, "PATH");
            tx.failure();
        }
        testIsTypeName(singletonMap("a", 10), "MAP");
        testIsTypeName(asList(1, 2), "LIST");
        testIsTypeName(1L, "INTEGER");
        testIsTypeName(1, "INTEGER");
        testIsTypeName(1.0D, "FLOAT");
        testIsTypeName(1.0, "FLOAT");
        testIsTypeName("a", "STRING");
        testIsTypeName(false, "BOOLEAN");
        testIsTypeName(true, "BOOLEAN");
        testIsTypeName(null, "NULL");
    }
    @Test
    public void testMetaTypes() throws Exception {

        Map<String, Object> param = map("MAP", singletonMap("a", 10),
                "LIST", asList(1, 2),
                "INTEGER", 1L,
                "FLOAT", 1.0D,
                "STRING", "a",
                "BOOLEAN", true,
                "NULL", null);
        TestUtil.testCall(db, "RETURN apoc.meta.types({param}) AS value", singletonMap("param",param), row -> {
            Map<String,String> res = (Map) row.get("value");
            res.forEach(Assert::assertEquals);
        });

    }

    private void testTypeName(Object value, String type) {
        TestUtil.testCall(db, "RETURN apoc.meta.typeName({value}) AS value", singletonMap("value", value), row -> assertEquals(type, row.get("value")));
//        TestUtil.testCall(db, "RETURN apoc.meta.type({value}) AS value", singletonMap("value", value), row -> assertEquals(type, row.get("value")));
    }

    private void testIsTypeName(Object value, String type) {
        TestUtil.testCall(db, "RETURN apoc.meta.isType({value},{type}) AS value", map("value", value, "type", type), result -> assertEquals("type was not "+type,true, result.get("value")));
        TestUtil.testCall(db, "RETURN apoc.meta.isType({value},{type}) AS value", map("value", value, "type", type + "foo"), result -> assertEquals(false, result.get("value")));
    }

    @Test
    public void testMetaStats() throws Exception {
        db.execute("CREATE (:Actor)-[:ACTED_IN]->(:Movie) ").close();
        TestUtil.testCall(db, "CALL apoc.meta.stats()", r -> {
            assertEquals(2L,r.get("labelCount"));
            assertEquals(1L,r.get("relTypeCount"));
            assertEquals(0L,r.get("propertyKeyCount"));
            assertEquals(2L,r.get("nodeCount"));
            assertEquals(1L,r.get("relCount"));
            assertEquals(map("Actor",1L,"Movie",1L),r.get("labels"));
            assertEquals(map(
                    "(:Actor)-[:ACTED_IN]->()",1L,
                    "()-[:ACTED_IN]->(:Movie)",1L,
                    "()-[:ACTED_IN]->()",1L),r.get("relTypes"));
        });
    }
    @Test
    public void testMetaGraph() throws Exception {
        db.execute("CREATE (a:Actor)-[:ACTED_IN]->(m1:Movie),(a)-[:ACTED_IN]->(m2:Movie)").close();
        TestUtil.testCall(db, "CALL apoc.meta.graph()",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    Node n1 = nodes.get(0);
                    assertEquals(true, n1.hasLabel(Label.label("Actor")));
                    assertEquals(1L, n1.getProperty("count"));
                    assertEquals("Actor", n1.getProperty("name"));
                    Node n2 = nodes.get(1);
                    assertEquals(true, n2.hasLabel(Label.label("Movie")));
                    assertEquals("Movie", n2.getProperty("name"));
                    assertEquals(2L, n2.getProperty("count"));
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    Relationship rel = rels.iterator().next();
                    assertEquals("ACTED_IN", rel.getType().name());
                    assertEquals(2L, rel.getProperty("count"));
                });
    }

    @Test
    public void testMetaGraph2() throws Exception {
        db.execute("CREATE (:Actor)-[:ACTED_IN]->(:Movie) ").close();
        TestUtil.testCall(db, "CALL apoc.meta.graphSample()",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    Node n1 = nodes.get(0);
                    assertEquals(true, n1.hasLabel(Label.label("Actor")));
                    assertEquals(1L, n1.getProperty("count"));
                    assertEquals("Actor", n1.getProperty("name"));
                    Node n2 = nodes.get(1);
                    assertEquals(true, n2.hasLabel(Label.label("Movie")));
                    assertEquals("Movie", n2.getProperty("name"));
                    assertEquals(1L, n1.getProperty("count"));
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    Relationship rel = rels.iterator().next();
                    assertEquals("ACTED_IN", rel.getType().name());
                    assertEquals(1L, rel.getProperty("count"));
                });
    }

    @Test
    public void testMetaData() throws Exception {
        db.execute("create index on :Movie(title)").close();
        db.execute("create constraint on (a:Actor) assert a.name is unique").close();
        db.execute("CREATE (:Actor {name:'Tom Hanks'})-[:ACTED_IN {roles:'Forrest'}]->(:Movie {title:'Forrest Gump'}) ").close();
        TestUtil.testResult(db, "CALL apoc.meta.data()",
                (r) -> {
                    int count = 0;
                    while (r.hasNext()) {
                        Map<String, Object> row = r.next();
                        // todo more assertions
                        count ++;
                    }
                    assertEquals(5,count);
                });
    }

    @Test
    public void testRelTypePropertiesBasic() throws Exception {
        db.execute("CREATE (:Base)-[:RELTYPE { a: 1, d: null }]->(:Target)").close();
        db.execute("CREATE (:Base)-[:RELTYPE { a: 2, b: 2, c: 2, d: 4 }]->(:Target);").close();

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties()", r -> {
            List<Map<String,Object>> records = gatherRecords(r);

            System.out.println("REL TYPE PROPERTIES");
            System.out.println(toCSV(records));

            assertEquals(true, hasRecordMatching(records, m ->
                    m.get("propertyName").equals("a") &&
                            ((List)m.get("propertyTypes")).get(0).equals("Long") &&
                            m.get("mandatory").equals(false)));

            assertEquals(true, hasRecordMatching(records, m ->
                    m.get("propertyName").equals("b") &&
                            ((List)m.get("propertyTypes")).get(0).equals("Long") &&
                            m.get("mandatory").equals(false)));

            assertEquals(true, hasRecordMatching(records, m ->
                    m.get("propertyName").equals("c") &&
                            ((List)m.get("propertyTypes")).get(0).equals("Long") &&
                            m.get("mandatory").equals(false)));

            assertEquals(true, hasRecordMatching(records, m ->
                    m.get("propertyName").equals("d") &&
                            ((List)m.get("propertyTypes")).get(0).equals("Long") &&
                            m.get("mandatory").equals(false)));
        });
    }

    @Test
    public void testRelTypePropertiesIncludes() throws Exception {
        db.execute("CREATE (:A)-[:CATCHME { c: 1 }]->(:B)").close();
        db.execute("CREATE (:A)-[:IGNOREME { d: 1 }]->(:B)").close();

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ includeRels: ['CATCHME'] })", r -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertEquals(records.get(0).get("propertyName").equals("c"), true);
        });
    }

    @Test
    public void testNodeTypePropertiesNodeExcludes() throws Exception {
        db.execute("CREATE (:ExcludeMe)").close();
        db.execute("CREATE (:IncludeMe)").close();

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ excludeLabels: ['ExcludeMe'] })", r -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertEquals(true, records.get(0).get("nodeType").equals(":`IncludeMe`"));
        });
    }

    @Test
    public void testNodeTypePropertiesNodeIncludes() throws Exception {
        db.execute("CREATE (:ExcludeMe)").close();
        db.execute("CREATE (:IncludeMe)").close();

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ includeLabels: ['IncludeMe'] })", r -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertEquals(true, records.get(0).get("nodeType").equals(":`IncludeMe`"));
        });
    }

    @Test
    public void testNodeTypePropertiesRelExcludes() throws Exception {
        db.execute("CREATE (:A)-[:RELA { x: 1 }]->(:C)").close();
        db.execute("CREATE (:B)-[:RELB { x: 1 }]->(:D)").close();

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ excludeRels: ['RELA'] })", r -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertEquals(2, records.size());
            for (Map<String,Object> rec : records) {
                if (rec.get("nodeType").equals(":`A`")) {
                    assertEquals(true, rec.get("nodeType").equals(":`B`"));
                }
                if (rec.get("nodeType").equals(":`C`")) {
                    assertEquals(true, rec.get("nodeType").equals(":`D`"));
                }
            }
        });
    }

    @Test
    public void testNodeTypePropertiesRelIncludes() throws Exception {
        db.execute("CREATE (:A)-[:RELA { x: 1 }]->(:C)").close();
        db.execute("CREATE (:B)-[:RELB { x: 1 }]->(:D)").close();

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ includeRels: ['RELA'] })", r -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertEquals(2, records.size());
            for (Map<String,Object> rec : records) {
                if (rec.get("nodeType").equals(":`A`")) {
                    assertEquals(true, rec.get("nodeType").equals(":`A`"));
                }
                if (rec.get("nodeType").equals(":`C`")) {
                    assertEquals(true, rec.get("nodeType").equals(":`C`"));
                }
            }
        });
    }

    @Test
    public void testRelTypePropertiesRelExcludes() throws Exception {
        db.execute("CREATE (:A)-[:RELA { x: 1 }]->(:C)").close();
        db.execute("CREATE (:B)-[:RELB { x: 1 }]->(:D)").close();

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ excludeRels: ['RELA'] })", r -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertEquals(true, records.get(0).get("relType").equals(":`RELB`"));
        });
    }

    @Test
    public void testRelTypePropertiesRelIncludes() throws Exception {
        db.execute("CREATE (:A)-[:RELA { x: 1 }]->(:C)").close();
        db.execute("CREATE (:B)-[:RELB { x: 1 }]->(:D)").close();

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ includeRels: ['RELA'] })", r -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertEquals(true, records.get(0).get("relType").equals(":`RELA`"));
        });
    }

    @Test
    public void testRelTypePropertiesNodeExcludes() throws Exception {
        db.execute("CREATE (:A)-[:RELA { x: 1 }]->(:C)").close();
        db.execute("CREATE (:B)-[:RELB { x: 1 }]->(:D)").close();

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ excludeLabels: ['A'] })", r -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String,Object> rec : records) {
                if (rec.get("relType").equals(":`RELB`")) {
                    assertEquals(true, rec.get("relType").equals(":`RELB`"));
                }
            }
        });
    }

    @Test
    public void testRelTypePropertiesNodeIncludes() throws Exception {
        db.execute("CREATE (:A)-[:RELA { x: 1 }]->(:C)").close();
        db.execute("CREATE (:B)-[:RELB { x: 1 }]->(:D)").close();

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ includeLabels: ['A'] })", r -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String,Object> rec : records) {
                if (rec.get("relType").equals(":`RELA`")) {
                    assertEquals(true, rec.get("relType").equals(":`RELA`"));
                }
            }
        });
    }

    @Test
    public void testNodeTypePropertiesEquivalenceAdvanced() throws Exception {
        db.execute("CREATE (:Foo { l: 1, s: 'foo', d: datetime(), ll: ['a', 'b'], dl: [2.0, 3.0] });").close();
        // Missing all properties to make everything non-mandatory.
        db.execute("CREATE (:Foo { z: 1 });").close();
        assertEquals(true, testDBCallEquivalence(db, "CALL apoc.meta.nodeTypeProperties()", "CALL db.schema.nodeTypeProperties()"));
    }

    @Test
    public void testNodeTypePropertiesEquivalenceTypeMapping() throws Exception {
        String q =
            "CREATE (:Test {" +
            "    longProp: 1," +
            "    doubleProp: 3.14," +
            "    stringProp: 'Hello'," +
            "    longArrProp: [1,2,3]," +
            "    doubleArrProp: [3.14, 3.14]," +
            "    stringArrProp: ['Hello', 'World']," +
            "    dateTimeProp: datetime()," +
            "    dateProp: date()," +
            "    pointProp: point({ x:0, y:4, z:1 })," +
            "    pointArrProp: [point({ x:0, y:4, z:1 }), point({ x:0, y:4, z:1 })]," +
            "    boolProp: true," +
            "    boolArrProp: [true, false]\n" +
            "})" + 
            "CREATE (:Test { randomProp: 'this property is here to make everything mandatory = false'});";
        

        db.execute(q).close();
        assertEquals(true, testDBCallEquivalence(db, "CALL apoc.meta.nodeTypeProperties()", "CALL db.schema.nodeTypeProperties()"));
    }

    /*

    neoarchitect: removing this test for now as the output of the apoc versions has changed significantly.

    public void setupAdvancedScenario(GraphDatabaseService db) {
        // Purpose of this is to have multiple overlapping labels, differing type information, and partially present
        // attributes, and test that the behavior is the same.
        String [] queries = new String [] {
                "CREATE (p1:Person { name: 'Bob', age: 12, dob: date() })-[:FRIENDS]->(p2:Person:Friend { name: 'Sam' })",
                "CREATE (:Person { name: 'Zeke', age: 13, weirdo: [point({x:1,y:2,z:3})] })-[:FRIENDS { since: datetime() }]->(:Person:Enemy { name: 'Hep' })",
                "CREATE (:Person { randomProp: 'this property is here to make everything mandatory = false'})",
        };

        for(String q : queries) { db.execute(q).close(); }
    }

    @Test
    public void testNodeTypePropertiesAdvancedScenario() throws Exception {
        setupAdvancedScenario(db);
        assertEquals(true, testDBCallEquivalence(db, "CALL apoc.meta.nodeTypeProperties()", "CALL db.schema.nodeTypeProperties()"));
        assertEquals(true, testDBCallEquivalence(db, "CALL apoc.meta.relTypeProperties()", "CALL db.schema.relTypeProperties()"));
    } */

    private static String toCSV(List<Map<String, Object>> list) {
        List<String> headers = list.stream().flatMap(map -> map.keySet().stream()).distinct().collect(Collectors.toList());
        final StringBuffer sb = new StringBuffer();
        for (int i = 0; i < headers.size(); i++) {
            sb.append(headers.get(i));
            sb.append(i == headers.size()-1 ? "\n" : ",");
        }
        for (Map<String, Object> map : list) {
            for (int i = 0; i < headers.size(); i++) {
                sb.append(map.get(headers.get(i)));
                sb.append(i == headers.size()-1 ? "\n" : ",");
            }
        }
        return sb.toString();
    }

    public static boolean testDBCallEquivalence(GraphDatabaseService db, String testCall, String equivalentToCall) {
        AtomicReference<List<Map<String,Object>>> compareTo = new AtomicReference<>();
        AtomicReference<List<Map<String,Object>>> testSet = new AtomicReference<>();

        TestUtil.testResult(db, equivalentToCall, r -> {
            compareTo.set(gatherRecords(r));
        });

        TestUtil.testResult(db, testCall, r -> {
            testSet.set(gatherRecords(r));
        });

        System.out.println("COMPARE TO:");
        System.out.println(toCSV(compareTo.get()));

        System.out.println("TEST SET:");
        System.out.println(toCSV(testSet.get()));

        return resultSetsEquivalent(compareTo.get(), testSet.get());
    }

    public static boolean resultSetsEquivalent(List<Map<String,Object>> baseSet, List<Map<String,Object>> testSet) {
        if (baseSet.size() != testSet.size()) {
            System.err.println("Result sets have different cardinality");
            return false;
        }

        boolean allMatch = true;

        for(Map<String,Object> baseRecord : baseSet) {
            allMatch = allMatch && hasRecordMatching(testSet, baseRecord);
        }

        return allMatch;
    }

    public static boolean hasRecordMatching(List<Map<String,Object>> records, Map<String,Object> record) {
        return hasRecordMatching(records, row -> {
            boolean okSoFar = true;

            for(String k : record.keySet()) {
                okSoFar = okSoFar && row.containsKey(k) &&
                        (row.get(k) == null ?
                                (record.get(k) == null) :
                                row.get(k).equals(record.get(k)));
            }

            return okSoFar;
        });
    }

    public static boolean hasRecordMatching(List<Map<String,Object>> records, Predicate<Map<String,Object>> predicate) {
        return records.stream().filter(predicate).count() > 0;
    }

    public static List<Map<String,Object>> gatherRecords(Result r) {
        List<Map<String,Object>> rows = new ArrayList<>();
        while(r.hasNext()) {
            Map<String,Object> row = r.next();
            rows.add(row);
        }
        return rows;
    }

    @Test
    public void testMetaSchema() {
        db.execute("create index on :Movie(title)").close();
        db.execute("create constraint on (p:Person) assert p.name is unique").close();
        db.execute("CREATE (:Person:Actor:Director {name:'Tom', born:'05-06-1956', dead:false})-[:ACTED_IN {roles:'Forrest'}]->(:Movie {title:'Forrest Gump'})").close();
        testCall(db, "CALL apoc.meta.schema()",
                (row) -> {
                    List<String> emprtyList = new ArrayList<String>();
                    List<String> fullList = Arrays.asList("Actor","Director");

                    Map<String, Object> o = (Map<String, Object>) row.get("value");
                    assertEquals(5, o.size());

                    Map<String, Object>  movie = (Map<String, Object>) o.get("Movie");
                    Map<String, Object>  movieProperties = (Map<String, Object>) movie.get("properties");
                    Map<String, Object>  movieTitleProperties = (Map<String, Object>) movieProperties.get("title");
                    assertNotNull(movie);
                    assertEquals("node", movie.get("type"));
                    assertEquals(1L, movie.get("count"));
                    assertEquals(emprtyList, movie.get("labels"));
                    assertEquals(4, movieTitleProperties.size());
                    assertEquals("STRING", movieTitleProperties.get("type"));
                    assertEquals(true, movieTitleProperties.get("indexed"));
                    assertEquals(false, movieTitleProperties.get("unique"));
                    Map<String, Object> movieRel = (Map<String, Object>) movie.get("relationships");
                    Map<String, Object> movieActedIn = (Map<String, Object>)movieRel.get("ACTED_IN");
                    assertEquals(1L, movieRel.size());
                    assertEquals("in", movieActedIn.get("direction"));
                    assertEquals(1L, movieActedIn.get("count"));
                    assertEquals(Arrays.asList("Person", "Actor", "Director"), movieActedIn.get("labels"));

                    Map<String, Object>  person = (Map<String, Object>) o.get("Person");
                    Map<String, Object>  personProperties = (Map<String, Object>) person.get("properties");
                    Map<String, Object>  personNameProperty = (Map<String, Object>) personProperties.get("name");
                    assertNotNull(person);
                    assertEquals("node", person.get("type"));
                    assertEquals(1L, person.get("count"));
                    assertEquals(fullList, person.get("labels"));
                    assertEquals(true, personNameProperty.get("unique"));
                    assertEquals(3, personProperties.size());

                    Map<String, Object>  actor = (Map<String, Object>) o.get("Actor");
                    assertNotNull(actor);
                    assertEquals("node", actor.get("type"));
                    assertEquals(1L, actor.get("count"));
                    assertEquals(emprtyList, actor.get("labels"));

                    Map<String, Object>  director = (Map<String, Object>) o.get("Director");
                    Map<String, Object>  directorProperties = (Map<String, Object>) director.get("properties");
                    assertNotNull(director);
                    assertEquals("node", director.get("type"));
                    assertEquals(1L, director.get("count"));
                    assertEquals(emprtyList, director.get("labels"));
                    assertEquals(3, directorProperties.size());

                    Map<String, Object>  actedIn = (Map<String, Object>) o.get("ACTED_IN");
                    Map<String, Object>  actedInProperties = (Map<String, Object>) actedIn.get("properties");
                    Map<String, Object>  actedInRoleProperty = (Map<String, Object>) actedInProperties.get("roles");
                    assertNotNull(actedIn);
                    assertEquals("relationship", actedIn.get("type"));
                    assertEquals("STRING", actedInRoleProperty.get("type"));
                    assertEquals(false, actedInRoleProperty.get("array"));
                    assertEquals(false, actedInRoleProperty.get("existence"));
                });
    }

    @Test
    public void testSubGraphNoLimits() throws Exception {
        db.execute("CREATE (:A)-[:X]->(b:B),(b)-[:Y]->(:C)").close();
        testCall(db,"CALL apoc.meta.subGraph({})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(3, nodes.size());
            assertEquals(true, nodes.stream().map(n -> Iterables.first(n.getLabels()).name()).allMatch(n -> n.equals("A") || n.equals("B") || n.equals("C")));
            assertEquals(2, rels.size());
            assertEquals(true, rels.stream().map(r -> r.getType().name()).allMatch(n -> n.equals("X") || n.equals("Y")));
        });
    }
    @Test
    public void testSubGraphLimitLabels() throws Exception {
        db.execute("CREATE (:A)-[:X]->(b:B),(b)-[:Y]->(:C)").close();
        testCall(db,"CALL apoc.meta.subGraph({labels:['A','B']})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(2, nodes.size());
            assertEquals(true, nodes.stream().map(n -> Iterables.first(n.getLabels()).name()).allMatch(n -> n.equals("A") || n.equals("B")));
            assertEquals(1, rels.size());
            assertEquals(true, rels.stream().map(r -> r.getType().name()).allMatch(n -> n.equals("X")));
        });
    }
    @Test
    public void testSubGraphLimitRelTypes() throws Exception {
        db.execute("CREATE (:A)-[:X]->(b:B),(b)-[:Y]->(:C)").close();
        testCall(db,"CALL apoc.meta.subGraph({rels:['X']})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(3, nodes.size());
            assertEquals(true, nodes.stream().map(n -> Iterables.first(n.getLabels()).name()).allMatch(n -> n.equals("A") || n.equals("B") || n.equals("C")));
            assertEquals(1, rels.size());
            assertEquals(true, rels.stream().map(r -> r.getType().name()).allMatch(n -> n.equals("X")));
        });
    }
    @Test
    public void testSubGraphExcludes() throws Exception {
        db.execute("CREATE (:A)-[:X]->(b:B),(b)-[:Y]->(:C)").close();
        testCall(db,"CALL apoc.meta.subGraph({excludes:['B']})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(2, nodes.size());
            assertEquals(true, nodes.stream().map(n -> Iterables.first(n.getLabels()).name()).allMatch(n -> n.equals("A") || n.equals("C")));
            assertEquals(0, rels.size());
        });
    }

    @Test
    public void testMetaDate() throws Exception {

        Map<String, Object> param = map(
                "DATE", DateValue.now(Clock.systemDefaultZone()),
                "LOCAL_DATE", LocalDateTimeValue.now(Clock.systemDefaultZone()),
                "TIME", TimeValue.now(Clock.systemDefaultZone()),
                "LOCAL_TIME", LocalTimeValue.now(Clock.systemDefaultZone()),
                "DATE_TIME", DateTimeValue.now(Clock.systemDefaultZone()),
                "NULL", null);

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types({param}) AS value", singletonMap("param",param), row -> {
            Map<String, Object>  r = (Map<String, Object>) row.get("value");

            assertEquals("DATE", r.get("DATE"));
            assertEquals("LOCAL_DATE_TIME", r.get("LOCAL_DATE"));
            assertEquals("TIME", r.get("TIME"));
            assertEquals("LOCAL_TIME", r.get("LOCAL_TIME"));
            assertEquals("DATE_TIME", r.get("DATE_TIME"));
            assertEquals("NULL", r.get("NULL"));
        });
    }

    @Test
    public void testMetaArray() throws Exception {

        Map<String, Object> param = map(
                "ARRAY", new String[]{"a","b","c"},
                "ARRAY_FLOAT", new Float[]{1.2f, 2.2f},
                "ARRAY_DOUBLE", new Double[]{1.2, 2.2},
                "ARRAY_INT", new Integer[]{1, 2},
                "ARRAY_OBJECT", new Object[]{1, "a"},
                "ARRAY_POINT", new Object[]{Values.pointValue(CoordinateReferenceSystem.WGS84, 56.d, 12.78), Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.d, 12.78, 100)},
                "ARRAY_DURATION", new Object[]{isoDuration(5, 1, 43200, 0).asIsoDuration(), isoDuration(2, 1, 125454, 0).asIsoDuration()},
                "ARRAY_ARRAY", new Object[]{1, "a", new Object[]{"a", 1}, isoDuration(5, 1, 43200, 0).asIsoDuration()},
                "NULL", null);

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types({param}) AS value", singletonMap("param",param), row -> {
            Map<String, Object>  r = (Map<String, Object>) row.get("value");

            assertEquals("LIST OF STRING", r.get("ARRAY"));
            assertEquals("LIST OF FLOAT", r.get("ARRAY_FLOAT"));
            assertEquals("LIST OF FLOAT", r.get("ARRAY_DOUBLE"));
            assertEquals("LIST OF INTEGER", r.get("ARRAY_INT"));
            assertEquals("LIST OF ANY", r.get("ARRAY_OBJECT"));
            assertEquals("LIST OF POINT", r.get("ARRAY_POINT"));
            assertEquals("LIST OF DURATION", r.get("ARRAY_DURATION"));
            assertEquals("LIST OF ANY", r.get("ARRAY_ARRAY"));
            assertEquals("NULL", r.get("NULL"));
        });
    }

    @Test
    public void testMetaNumber() throws Exception {

        Map<String, Object> param = map(
                "INTEGER", 1L,
                "FLOAT", 1.0f,
                "DOUBLE", 1.0D,
                "NULL", null);

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types({param}) AS value", singletonMap("param",param), row -> {
            Map<String, Object>  r = (Map<String, Object>) row.get("value");

            assertEquals("INTEGER", r.get("INTEGER"));
            assertEquals("FLOAT", r.get("FLOAT"));
            assertEquals("FLOAT", r.get("DOUBLE"));
            assertEquals("NULL", r.get("NULL"));
        });
    }

    @Test
    public void testMeta() throws Exception {

        Map<String, Object> param = map(
                "LIST", asList(1.2, 2.1),
                "STRING", "a",
                "BOOLEAN", true,
                "CHAR", 'a',
                "DURATION", 'a',
                "POINT_2D",Values.pointValue(CoordinateReferenceSystem.WGS84, 56.d, 12.78),
                "POINT_3D", Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.7, 12.78, 100.0),
                "POINT_XYZ_2D", Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.3, 4.5),
                "POINT_XYZ_3D", Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 2.3, 4.5, 1.2),
                "DURATION", isoDuration(5, 1, 43200, 0).asIsoDuration(),
                "MAP", Util.map("a", "b"),
                "NULL", null);

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types({param}) AS value", singletonMap("param",param), row -> {
            Map<String, Object>  r = (Map<String, Object>) row.get("value");

            assertEquals("LIST OF FLOAT", r.get("LIST"));
            assertEquals("STRING", r.get("STRING"));
            assertEquals("BOOLEAN", r.get("BOOLEAN"));
            assertEquals("Character", r.get("CHAR"));
            assertEquals("POINT", r.get("POINT_2D"));
            assertEquals("POINT", r.get("POINT_3D"));
            assertEquals("POINT", r.get("POINT_XYZ_2D"));
            assertEquals("POINT", r.get("POINT_XYZ_3D"));
            assertEquals("DURATION", r.get("DURATION"));
            assertEquals("MAP", r.get("MAP"));
            assertEquals("NULL", r.get("NULL"));
        });
    }

    @Test
    public void testMetaList() throws Exception {

        Map<String, Object> param = map(
                "LIST FLOAT", asList(1.2F, 2.1F),
                "LIST STRING", asList("a", "b"),
                "LIST CHAR", asList('a', 'a'),
                "LIST DATE", asList(LocalDate.of(2018,1,1), LocalDate.of(2018,2,2)),
                "LIST ANY", asList("test",1,"asd",isoDuration(5, 1, 43200, 0).asIsoDuration()),
                "LIST NULL", asList("test",null),
                "LIST POINT", asList(Values.pointValue(CoordinateReferenceSystem.WGS84, 56.d, 12.78), Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 2.3, 4.5, 1.2)),
                "LIST DURATION", asList(isoDuration(5, 1, 43200, 0).asIsoDuration(), isoDuration(2, 1, 125454, 0).asIsoDuration()),
                "LIST OBJECT", new Object[]{LocalDate.of(2018,1,1), "test"},
                "LIST OF LIST", asList(asList("a", "b", "c"),asList("aa", "bb", "cc"),asList("aaa", "bbb", "ccc")),
                "LIST DOUBLE", asList(1.2D, 2.1D));

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types({param}) AS value", singletonMap("param",param), row -> {
            Map<String, Object>  r = (Map<String, Object>) row.get("value");

            assertEquals("LIST OF FLOAT", r.get("LIST FLOAT"));
            assertEquals("LIST OF STRING", r.get("LIST STRING"));
            assertEquals("LIST OF ANY", r.get("LIST CHAR"));
            assertEquals("LIST OF DATE", r.get("LIST DATE"));
            assertEquals("LIST OF FLOAT", r.get("LIST DOUBLE"));
            assertEquals("LIST OF POINT", r.get("LIST POINT"));
            assertEquals("LIST OF DURATION", r.get("LIST DURATION"));
            assertEquals("LIST OF ANY", r.get("LIST ANY"));
            assertEquals("LIST OF ANY", r.get("LIST OBJECT"));
            assertEquals("LIST OF LIST", r.get("LIST OF LIST"));
            assertEquals("LIST OF ANY", r.get("LIST NULL"));
        });
    }

    @Test
    public void testMetaPoint() throws Exception {
        db.execute("CREATE (:TEST {born:point({ longitude: 56.7, latitude: 12.78, height: 100 })})");

        TestUtil.testCall(db, "MATCH (t:TEST) WITH t.born as born RETURN apoc.meta.cypher.type(born) AS value", row -> assertEquals("POINT", row.get("value")));
    }

    @Test
    public void testMetaDuration() throws Exception {
        db.execute("CREATE (:TEST {duration:duration('P5M1DT12H')})");

        TestUtil.testCall(db, "MATCH (t:TEST) WITH t.duration as duration RETURN apoc.meta.cypher.type(duration) AS value", row -> assertEquals("DURATION", row.get("value")));
    }

    @Test
    public void testMetaDataWithSample() throws Exception {
        db.execute("create index on :Person(name)").close();
        db.execute("CREATE (:Person {name:'Tom'})").close();
        db.execute("CREATE (:Person {name:'John', surname:'Brown'})").close();
        db.execute("CREATE (:Person {name:'Nick'})").close();
        db.execute("CREATE (:Person {name:'Daisy', surname:'Bob'})").close();
        db.execute("CREATE (:Person {name:'Elizabeth'})").close();
        db.execute("CREATE (:Person {name:'Jack', surname:'White'})").close();
        db.execute("CREATE (:Person {name:'Joy'})").close();
        db.execute("CREATE (:Person {name:'Sarah', surname:'Taylor'})").close();
        db.execute("CREATE (:Person {name:'Jane'})").close();
        db.execute("CREATE (:Person {name:'Jeff', surname:'Logan'})").close();
        TestUtil.testResult(db, "CALL apoc.meta.data({sample:2})",
                (r) -> {
                    Map<String, Object>  personNameProperty = r.next();
                    Map<String, Object>  personSurnameProperty = r.next();
                    assertEquals("name", personNameProperty.get("property"));
                    assertEquals("surname", personSurnameProperty.get("property"));
                });
    }



    @Test
    public void testMetaDataWithSampleNormalized() throws Exception {
        db.execute("create index on :Person(name)").close();
        db.execute("CREATE (:Person {name:'Tom'})").close();
        db.execute("CREATE (:Person {name:'John'})").close();
        db.execute("CREATE (:Person {name:'Nick'})").close();
        db.execute("CREATE (:Person {name:'Daisy', surname:'Bob'})").close();
        db.execute("CREATE (:Person {name:'Elizabeth'})").close();
        db.execute("CREATE (:Person {name:'Jack'})").close();
        db.execute("CREATE (:Person {name:'Joy'})").close();
        db.execute("CREATE (:Person {name:'Sarah'})").close();
        db.execute("CREATE (:Person {name:'Jane'})").close();
        db.execute("CREATE (:Person {name:'Jeff', surname:'Logan'})").close();
        db.execute("CREATE (:City {name:'Milano'})").close();
        db.execute("CREATE (:City {name:'Roma'})").close();
        db.execute("CREATE (:City {name:'Firenze'})").close();
        db.execute("CREATE (:City {name:'Taormina', region:'Sicilia'})").close();
        TestUtil.testResult(db, "CALL apoc.meta.data({sample:5})",
                (r) -> {
                    Map<String, Object>  personNameProperty = r.next();
                    Map<String, Object>  personSurnameProperty = r.next();
                    assertEquals("Person", personNameProperty.get("label"));
                    assertEquals("name", personNameProperty.get("property"));
                    assertEquals("Person", personSurnameProperty.get("label"));
                    assertEquals("surname", personSurnameProperty.get("property"));

                    Map<String, Object>  cityNameProperty = r.next();
                    Map<String, Object>  cityRegionProperty = r.next();
                    assertEquals("City", cityNameProperty.get("label"));
                    assertEquals("name", cityNameProperty.get("property"));
                    assertEquals("City", cityRegionProperty.get("label"));
                    assertEquals("region", cityRegionProperty.get("property"));
                });
    }

    @Test
    public void testMetaDataWithSample5() throws Exception {
        db.execute("create index on :Person(name)").close();
        db.execute("CREATE (:Person {name:'John', surname:'Brown'})").close();
        db.execute("CREATE (:Person {name:'Daisy', surname:'Bob'})").close();
        db.execute("CREATE (:Person {name:'Nick'})").close();
        db.execute("CREATE (:Person {name:'Jack', surname:'White'})").close();
        db.execute("CREATE (:Person {name:'Elizabeth'})").close();
        db.execute("CREATE (:Person {name:'Joy'})").close();
        db.execute("CREATE (:Person {name:'Sarah', surname:'Taylor'})").close();
        db.execute("CREATE (:Person {name:'Jane'})").close();
        db.execute("CREATE (:Person {name:'Jeff', surname:'Logan'})").close();
        db.execute("CREATE (:Person {name:'Tom'})").close();
        TestUtil.testResult(db, "CALL apoc.meta.data({sample:5})",
                (r) -> {
                    Map<String, Object>  personNameProperty = r.next();
                    assertEquals("name", personNameProperty.get("property"));
                });
    }

    @Test
    public void testSchemaWithSample() {
        db.execute("create constraint on (p:Person) assert p.name is unique").close();
        db.execute("CREATE (:Person {name:'Tom'})").close();
        db.execute("CREATE (:Person {name:'John', surname:'Brown'})").close();
        db.execute("CREATE (:Person {name:'Nick'})").close();
        db.execute("CREATE (:Person {name:'Daisy', surname:'Bob'})").close();
        db.execute("CREATE (:Person {name:'Elizabeth'})").close();
        db.execute("CREATE (:Person {name:'Jack', surname:'White'})").close();
        db.execute("CREATE (:Person {name:'Joy'})").close();
        db.execute("CREATE (:Person {name:'Sarah', surname:'Taylor'})").close();
        db.execute("CREATE (:Person {name:'Jane'})").close();
        db.execute("CREATE (:Person {name:'Jeff', surname:'Logan'})").close();
        testCall(db, "CALL apoc.meta.schema({sample:2})",
                (row) -> {

                    Map<String, Object> o = (Map<String, Object>) row.get("value");
                    assertEquals(1, o.size());

                    Map<String, Object>  person = (Map<String, Object>) o.get("Person");
                    Map<String, Object>  personProperties = (Map<String, Object>) person.get("properties");
                    Map<String, Object>  personNameProperty = (Map<String, Object>) personProperties.get("name");
                    Map<String, Object>  personSurnameProperty = (Map<String, Object>) personProperties.get("surname");
                    assertNotNull(person);
                    assertEquals("node", person.get("type"));
                    assertEquals(10L, person.get("count"));
                    assertEquals("STRING", personNameProperty.get("type"));
                    assertEquals(false, personSurnameProperty.get("unique"));
                    assertEquals("STRING", personSurnameProperty.get("type"));
                    assertEquals(2, personProperties.size());

                });
    }

    @Test
    public void testSchemaWithSample5() {
        db.execute("create constraint on (p:Person) assert p.name is unique").close();
        db.execute("CREATE (:Person {name:'Tom'})").close();
        db.execute("CREATE (:Person {name:'John', surname:'Brown'})").close();
        db.execute("CREATE (:Person {name:'Nick'})").close();
        db.execute("CREATE (:Person {name:'Daisy', surname:'Bob'})").close();
        db.execute("CREATE (:Person {name:'Elizabeth'})").close();
        db.execute("CREATE (:Person {name:'Jack', surname:'White'})").close();
        db.execute("CREATE (:Person {name:'Joy'})").close();
        db.execute("CREATE (:Person {name:'Sarah', surname:'Taylor'})").close();
        db.execute("CREATE (:Person {name:'Jane'})").close();
        db.execute("CREATE (:Person {name:'Jeff', surname:'Logan'})").close();
        testCall(db, "CALL apoc.meta.schema({sample:5})",
                (row) -> {

                    Map<String, Object> o = (Map<String, Object>) row.get("value");
                    assertEquals(1, o.size());
                    Map<String, Object>  person = (Map<String, Object>) o.get("Person");
                    Map<String, Object>  personProperties = (Map<String, Object>) person.get("properties");
                    Map<String, Object>  personNameProperty = (Map<String, Object>) personProperties.get("name");
                    assertNotNull(person);
                    assertEquals("node", person.get("type"));
                    assertEquals(10L, person.get("count"));
                    assertEquals("STRING", personNameProperty.get("type"));
                    assertEquals(true, personNameProperty.get("unique"));
                    assertEquals(true, personProperties.size() >= 1);

                });
    }

    @Test
    public void testMetaGraphExtraRelsWithSample() throws Exception {
        db.execute("CREATE (:S1 {name:'Tom'})").close();
        db.execute("CREATE (:S2 {name:'John', surname:'Brown'})-[:KNOWS{since:2012}]->(:S7)").close();
        db.execute("CREATE (:S1 {name:'Nick'})").close();
        db.execute("CREATE (:S3 {name:'Daisy', surname:'Bob'})-[:KNOWS{since:2012}]->(:S7)").close();
        db.execute("CREATE (:S1 {name:'Elizabeth'})").close();
        db.execute("CREATE (:S4 {name:'Jack', surname:'White'})-[:KNOWS{since:2012}]->(:S7)").close();
        db.execute("CREATE (:S1 {name:'Joy'})").close();
        db.execute("CREATE (:S5 {name:'Sarah', surname:'Taylor'})-[:KNOWS{since:2012}]->(:S7)").close();
        db.execute("CREATE (:S1 {name:'Jane'})").close();
        db.execute("CREATE (:S6 {name:'Jeff', surname:'Logan'})-[:KNOWS{since:2012}]->(:S7)").close();

        testCall(db, "call apoc.meta.graph({sample:2})",(row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            assertEquals(7,nodes.size());
        });
    }

}
