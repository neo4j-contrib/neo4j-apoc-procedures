package apoc.export.graphml;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.Util;
import junit.framework.TestCase;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportGraphMLTest {

    public static final String KEY_TYPES = "<key id=\"values\" for=\"node\" attr.name=\"values\" attr.type=\"string\" attr.list=\"long\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"long\"/>%n";
    public static final String GRAPH = "<graph id=\"G\" edgedefault=\"directed\">%n";
    public static final String HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n" +
            "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">%n";
    public static final String DATA = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"labels\">:Foo:Foo0:Foo2</data><data key=\"name\">foo</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"labels\">:Bar</data><data key=\"name\">bar</data><data key=\"age\">42</data></node>%n" +
            "<node id=\"n2\" labels=\":Bar\"><data key=\"labels\">:Bar</data><data key=\"age\">12</data><data key=\"values\">[1,2,3]</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data></edge>%n";

    public static final String DATA_NODE_EDGE = "<node id=\"n0\"> <data key=\"labels\">:FOO</data><data key=\"name\">foo</data> </node>%n" +
            "<node id=\"n1\"> <data key=\"labels\">:BAR</data><data key=\"name\">bar</data> <data key=\"kids\">[a,b,c]</data> </node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\"> <data key=\"label\">:EDGE_LABEL</data> <data key=\"name\">foo</data> </edge>%n" +
            "<edge id=\"e1\" source=\"n1\" target=\"n0\"><data key=\"label\">TEST</data> </edge>%n" +
            "<node id=\"n3\"> <data key=\"labels\">:QWERTY</data><data key=\"name\">qwerty</data> </node>%n" +
            "<edge id=\"e2\" source=\"n1\" target=\"n3\"> <data key=\"label\">KNOWS</data> </edge>%n";

    public static final String FOOTER = "</graph>%n" +
            "</graphml>";
    private static final String EXPECTED = String.format(HEADER + GRAPH + DATA + FOOTER);
    private static final String EXPECTED_TYPES = String.format(HEADER + KEY_TYPES +GRAPH + DATA + FOOTER);
    private static final String EXPECTED_READ_NODE_EDGE = String.format(HEADER + GRAPH + DATA_NODE_EDGE + FOOTER);

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public TestName testName = new TestName();

    private static final String TEST_WITH_NO_IMPORT = "WithNoImportConfig";
    private static final String TEST_WITH_NO_EXPORT = "WithNoExportConfig";

    @Before
    public void setUp() throws Exception {
        GraphDatabaseBuilder builder  = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath());
        if (!testName.getMethodName().endsWith(TEST_WITH_NO_EXPORT)) {
            builder.setConfig("apoc.export.file.enabled", "true");
        }
        if (!testName.getMethodName().endsWith(TEST_WITH_NO_IMPORT)) {
            builder.setConfig("apoc.import.file.enabled", "true");
        }
        db = builder.newGraphDatabase();
        TestUtil.registerProcedure(db, ExportGraphML.class, Graphs.class);
        db.execute("CREATE (f:Foo:Foo2:Foo0 {name:'foo'})-[:KNOWS]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12,values:[1,2,3]})").close();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testImportGraphML() throws Exception {
        db.execute("MATCH (n) DETACH DELETE n").close();

        File output = new File(directory, "import.graphml");
        FileWriter fw = new FileWriter(output);
        fw.write(EXPECTED_TYPES); fw.close();
        TestUtil.testCall(db, "CALL apoc.import.graphml({file},{readLabels:true})", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));

        TestUtil.testCall(db, "MATCH  (c:Bar {age: 12, values: [1,2,3]}) RETURN COUNT(c) AS c", null, (r) -> assertEquals(1L, r.get("c")));
    }

    @Test(expected = QueryExecutionException.class)
    public void testImportGraphMLWithNoImportConfig() throws Exception {
        File output = new File(directory, "all.graphml");
        try {
            TestUtil.testCall(db, "CALL apoc.import.graphml({file},{readLabels:true})", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "database"));
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals("Import from files not enabled, please set apoc.import.file.enabled=true in your neo4j.conf", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testImportGraphMLNodeEdge() throws Exception {
        db.execute("MATCH (n) DETACH DELETE n").close();

        File output = new File(directory, "importNodeEdges.graphml");
        FileWriter fw = new FileWriter(output);
        fw.write(EXPECTED_READ_NODE_EDGE); fw.close();
        TestUtil.testCall(db, "CALL apoc.import.graphml({file},{readLabels:true})", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(3L, r.get("nodes"));
                    assertEquals(3L, r.get("relationships"));
                    assertEquals(5L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("database: nodes(3), rels(3)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });

        TestUtil.testCall(db, "MATCH (foo:FOO)-[rel:EDGE_LABEL]->(bar:BAR) RETURN foo, rel, bar", null, (r) -> {
            assertFoo(((Node)r.get("foo")));
            assertBar(((Node)r.get("bar")));
            assertEquals("EDGE_LABEL", ((Relationship)r.get("rel")).getType().name());
            assertEquals(Util.map("name", "foo"), ((Relationship)r.get("rel")).getAllProperties());
        });
        TestUtil.testCall(db, "MATCH (bar:BAR)-[test:TEST]->(foo:FOO) RETURN bar, test, foo", null, (r) -> {
            assertFoo(((Node)r.get("foo")));
            assertBar(((Node)r.get("bar")));
            assertEquals("TEST", ((Relationship)r.get("test")).getType().name());
        });
        TestUtil.testCall(db, "MATCH (bar:BAR)-[knows:KNOWS]->(qwerty:QWERTY) RETURN bar, knows, qwerty", null, (r) -> {
            assertBar(((Node)r.get("bar")));
            assertEquals(Arrays.asList(Label.label("QWERTY")), ((Node)r.get("qwerty")).getLabels());
            assertEquals(Util.map("name", "qwerty"), ((Node)r.get("qwerty")).getAllProperties());
            assertEquals("KNOWS", ((Relationship)r.get("knows")).getType().name());
        });

    }

    private void assertBar(Node node){
        assertEquals(Arrays.asList(Label.label("BAR")), node.getLabels());
        assertEquals(Util.map("name", "bar", "kids", "[a,b,c]"), node.getAllProperties());
    }

    private void assertFoo(Node node){
        assertEquals(Arrays.asList(Label.label("FOO")), node.getLabels());
        assertEquals(Util.map("name", "foo"), node.getAllProperties());
    }

    @Test
    public void testExportAllGraphML() throws Exception {
        File output = new File(directory, "all.graphml");
        TestUtil.testCall(db, "CALL apoc.export.graphml.all({file},null)", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphGraphML() throws Exception {
        File output = new File(directory, "graph.graphml");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, {file},null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }
    @Test
    public void testExportGraphGraphMLTypes() throws Exception {
        File output = new File(directory, "graph.graphml");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, {file},{useTypes:true}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_TYPES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test(expected = QueryExecutionException.class)
    public void testExportGraphGraphMLTypesWithNoExportConfig() throws Exception {
        File output = new File(directory, "all.graphml");
        try {
            TestUtil.testCall(db, "CALL apoc.export.graphml.all({file},null)", map("file", output.getAbsolutePath()),(r) -> assertResults(output, r, "database"));
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals("Export to files not enabled, please set apoc.export.file.enabled=true in your neo4j.conf", except.getMessage());
            throw e;
        }
    }

    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(5L, r.get("properties"));
        assertEquals(output.getAbsolutePath(), r.get("file"));
        if (r.get("source").toString().contains(":"))
            assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        else
            assertEquals("file", r.get("source"));
        assertEquals("graphml", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
    }
}