package apoc.export.graphml;

import apoc.util.BinaryTestUtil;
import apoc.util.CompressionAlgo;
import apoc.util.CompressionConfig;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import junit.framework.TestCase;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.ApocConfig.EXPORT_TO_FILE_ERROR;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_DATA;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_FALSE;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_READ_NODE_EDGE;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TINKER;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_EMPTY;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_NO_DATA_KEY;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_PATH;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_PATH_CAMEL_CASE;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_PATH_CAPTION;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_PATH_CAPTION_TINKER;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_PATH_WRONG_CAPTION;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_WITHOUT_CHAR_DATA_KEYS;
import static apoc.export.graphml.ExportGraphMLTestUtil.assertXMLEquals;
import static apoc.export.graphml.ExportGraphMLTestUtil.setUpGraphMl;
import static apoc.util.BinaryTestUtil.getDecompressedData;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.graphdb.Label.label;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportGraphMLTest {

    @Rule
    public TestName testName = new TestName();


    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
                .withSetting(GraphDatabaseSettings.memory_tracking, true)
                .withSetting(GraphDatabaseSettings.tx_state_memory_allocation, OFF_HEAP)
                .withSetting(GraphDatabaseSettings.tx_state_max_off_heap_memory, BYTES.parse("200m"))
                .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Before
    public void setUp() {
        setUpGraphMl(db, testName);
    }

    @Test
    public void testImportGraphML() throws Exception {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        File output = new File(directory, "import.graphml");
        FileWriter fw = new FileWriter(output);
        fw.write(EXPECTED_TYPES); fw.close();
        TestUtil.testCall(db, "CALL apoc.import.graphml($file,{readLabels:true})", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertResults(output, r, "statement");
                });

        TestUtil.testCall(db, "MATCH  (c:Bar {age: 12, values: [1,2,3]}) RETURN COUNT(c) AS c", null, (r) -> assertEquals(1L, r.get("c")));
    }
    
    @Test
    public void testRoundtripInvalidUnicode() {
        String fileName = new File(directory, "allUnicode.graphml").getAbsolutePath();
        final String query = "CALL apoc.export.graphml.all($file, null)";

        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        testRoundtripInvalidUnicodeCommon(query, fileName);
    }

    @Test
    public void testRoundtripInvalidUnicodeWithExportQuery() {
        String file = new File(directory, "allQuery.graphml").getAbsolutePath();
        // trello issue case: https://trello.com/c/6GboqUau/1070-s3cast-software-issue-with-apocimportgraphml
        final String query = "CALL apoc.export.graphml.query('MATCH (n:Unicode) RETURN n', $file, {useTypes: true, readLabels:true})";
        
        testRoundtripInvalidUnicodeCommon(query, file);
    }

    private void testRoundtripInvalidUnicodeCommon(String query, String file) {
        db.executeTransactionally("CREATE (n:Unicode $props)",
                map("props", map("propZero", "\u007FnotEscaped'", 
                        "propOne", "\u00101628\u000eX",
                        "propTwo", "abcde\u001ef",
                        "propThree", "aj\u0000eje>",
                        "propFour", "b\u0018ra\u000fz\u0001or'f",
                        "propFive", "a\u0014noth&r\uffff \" one")
                ));

        TestUtil.testCall(db, query, map("file", file),
                (r) -> assertEquals(1L, r.get("nodes")));

        db.executeTransactionally("MATCH (n:Unicode) DETACH DELETE n");
        testImportInvalidUnicode(file);
    }
    
    @Test
    public void testImportInvalidUnicodeFile() {
        final String file = ClassLoader.getSystemResource("fileWithUnicode.graphml").toString();
        testImportInvalidUnicode(file);
    }

    private void testImportInvalidUnicode(String file) {
        TestUtil.testCall(db, "CALL apoc.import.graphml($file, {readLabels:true})",  map("file", file),
                r -> {
                    assertEquals(true, r.get("done"));
                    assertEquals(1L, r.get("nodes"));
                });

        TestUtil.testCall(db, "MATCH (n:Unicode) RETURN n",
                r -> {
                    final Node node = (Node) r.get("n");
                    // valid unicode chars remain as they are
                    assertEquals("\u007FnotEscaped'", node.getProperty("propZero"));
                    assertEquals("1628X", node.getProperty("propOne"));
                    assertEquals("abcdef", node.getProperty("propTwo"));
                    assertEquals("ajeje>", node.getProperty("propThree"));
                    assertEquals("brazor'f", node.getProperty("propFour"));
                    assertEquals("anoth&r \" one", node.getProperty("propFive"));
                });

        db.executeTransactionally("MATCH (n:Unicode) DETACH DELETE n");
    }

    @Test
    public void testRoundTripWithSeparatedImport() {
        Map<String, Object> exportConfig = map("useTypes", true);

        Map<String, Object> importConfig = map("readLabels", true, "storeNodeIds", true,
                "source", map("label", "Foo"),
                "target", map("label", "Bar"));

        // we didn't specified a source/target in export config
        // so we have to store the nodeIds and looking for them during relationship import
        separatedFileCommons(exportConfig, importConfig);
    }

    @Test
    public void testImportSeparatedFilesWithCustomId() {
        Map<String, Object> exportConfig = map("useTypes", true,
                "source", map("id", "name"), 
                "target", map("id", "age"));
        
        Map<String, Object> importConfig = map("readLabels", true,
                "source", map("label", "Foo", "id", "name"), 
                "target", map("label", "Bar", "id", "age"));
        
        // we specified a source/target in export config
        // so storeNodeIds config is unnecessary and we search nodes by properties Foo.name and Bar.age
        separatedFileCommons(exportConfig, importConfig);
    }

    private void separatedFileCommons(Map<String, Object> exportConfig, Map<String, Object> importConfig) {
        db.executeTransactionally("CREATE (:Foo {name: 'zzz'})-[:KNOWS]->(:Bar {age: 0}), (:Foo {name: 'aaa'})-[:KNOWS {id: 1}]->(:Bar {age: 666})");

        // we export 3 files: 1 for source nodes, 1 for end nodes, 1 for relationships
        String outputNodesFoo = new File(directory, "queryNodesFoo.graphml").getAbsolutePath();
        String outputNodesBar = new File(directory, "queryNodesBar.graphml").getAbsolutePath();
        String outputRelationships = new File(directory, "queryRelationship.graphml").getAbsolutePath();

        TestUtil.testCall(db, "CALL apoc.export.graphml.query('MATCH (start:Foo)-[:KNOWS]->(:Bar) RETURN start',$file, $config)",
                map("file", outputNodesFoo, "config", exportConfig),
                (r) -> assertEquals(3L, r.get("nodes")));

        TestUtil.testCall(db, "CALL apoc.export.graphml.query('MATCH (:Foo)-[:KNOWS]->(end:Bar) RETURN end', $file, $config) ",
                map("file", outputNodesBar, "config", exportConfig),
                (r) -> assertEquals(3L, r.get("nodes")));

        TestUtil.testCall(db, "MATCH (:Foo)-[rel:KNOWS]->(:Bar) WITH collect(rel) as rels \n" +
                        "call apoc.export.graphml.data([], rels, $file, $config) " +
                        "YIELD nodes, relationships RETURN nodes, relationships",
                map("file", outputRelationships, "config", exportConfig),
                (r) -> assertEquals(3L, r.get("relationships")));

        // delete current entities and re-import
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        TestUtil.testCall(db, "CALL apoc.import.graphml($file, $config)",
                map("file", outputNodesFoo, "config", importConfig),
                (r) -> assertEquals(3L, r.get("nodes")));

        TestUtil.testCall(db, "CALL apoc.import.graphml($file, $config)",
                map("file", outputNodesBar, "config", importConfig),
                (r) -> assertEquals(3L, r.get("nodes")));

        TestUtil.testCall(db, "CALL apoc.import.graphml($file, $config)",
                map("file", outputRelationships, "config", importConfig),
                (r) -> assertEquals(3L, r.get("relationships")));

        TestUtil.testResult(db, "MATCH (start:Foo)-[rel:KNOWS]->(end:Bar) \n" +
                        "RETURN start.name AS startName, rel.id AS relId, end.age AS endAge \n" +
                        "ORDER BY start.name",
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertions(row, "aaa", 1L, 666L);
                    row = r.next();
                    assertions(row, "foo", null, 42L);
                    row = r.next();
                    assertions(row, "zzz", null, 0L);
                    assertFalse(r.hasNext());
                });
    }

    private void assertions(Map<String, Object> row, String expectedSource, Long expectedRel, Long expectedTarget) {
        assertEquals(expectedSource, row.get("startName"));
        assertEquals(expectedRel, row.get("relId"));
        assertEquals(expectedTarget, row.get("endAge"));
    }

    @Test
    public void testImportGraphMLLargeFile() {
        assumeFalse(isRunningInCI());
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        final String file = ClassLoader.getSystemResource("largeFile.graphml").toString();
        TestUtil.testCall(db, "CALL apoc.import.graphml($file,{readLabels:true})", map("file", file),
                (r) -> {
                    assertEquals(335160L, r.get("nodes"));
                    assertEquals(5666L, r.get("relationships"));
                    assertEquals(737297L, r.get("properties"));
                    assertEquals(file, r.get("file"));
                    assertEquals("graphml", r.get("format"));
                    assertEquals(true, r.get("done"));
                });
    }

    @Test
    public void testImportGraphMLWithEdgeWithoutDataKeys() throws Exception {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        File output = new File(directory, "import.graphml");
        FileWriter fw = new FileWriter(output);
        fw.write(EXPECTED_TYPES_NO_DATA_KEY); fw.close();
        TestUtil.testCall(db, "CALL apoc.import.graphml($file,{readLabels:true})", map("file", output.getAbsolutePath()),
                (r) -> assertEquals(2L, r.get("nodes")));

        TestUtil.testCall(db, "MATCH  ()-[c:RELATED]->() RETURN COUNT(c) AS c", null, (r) -> assertEquals(1L, r.get("c")));
    }


    @Test
    public void testImportGraphMLWithoutCharactersDataKeys() throws Exception {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        File output = new File(directory, "import.graphml");
        FileWriter fw = new FileWriter(output);
        fw.write(EXPECTED_TYPES_WITHOUT_CHAR_DATA_KEYS); fw.close();
        TestUtil.testCall(db, "CALL apoc.import.graphml($file,{readLabels:true})", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertResults(output, r, "statement");
                });

        TestUtil.testCall(db, "MATCH  ()-[c:RELATED]->() RETURN COUNT(c) AS c", null, (r) -> assertEquals(1L, r.get("c")));
    }

    private String NO_REL_TYPES = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" +
            "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            "         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" +
            "         http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" +
            "<!-- Created by igraph -->\n" +
            "  <key id=\"username\" for=\"node\" attr.name=\"username\" attr.type=\"string\"/>\n" +
            "  <key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"double\"/>\n" +
            "  <graph id=\"G\" edgedefault=\"directed\">\n" +
            "    <node id=\"n0\">\n" +
            "      <data key=\"username\">Dodo von den Bergen</data>\n" +
            "    </node>\n" +
            "    <node id=\"n1\">\n" +
            "      <data key=\"username\">Semolo75</data>\n" +
            "    </node>  " +
            "<edge source=\"n1\" target=\"n0\">\n" +
            "      <data key=\"weight\">1</data>\n" +
            "    </edge>  </graph>\n" +
            "</graphml>";
    
    @Test
    public void testImportDefaultRelationship() throws Exception {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        File output = new File(directory, "import_no_rel_types.graphml");
        FileWriter fw = new FileWriter(output);
        fw.write(NO_REL_TYPES); fw.close();
        db.executeTransactionally("CALL apoc.import.graphml( $file, { batchSize: 5000, readLabels: true, defaultRelationshipType:'DEFAULT_TYPE' })", map("file", output.getAbsolutePath()));
        TestUtil.testCall(db, "MATCH ()-[r]-() RETURN Distinct type(r) as type",
                (r) -> {
                    String label = (String) r.get("type");
                    assertEquals("DEFAULT_TYPE", label);
                }
        );
    }

    @Test(expected = QueryExecutionException.class)
    public void testImportGraphMLWithNoImportConfig() {
        File output = new File(directory, "all.graphml");
        try {
            TestUtil.testCall(db, "CALL apoc.import.graphml($file,{readLabels:true})", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "database"));
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals("Import from files not enabled, please set apoc.import.file.enabled=true in your apoc.conf", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testImportGraphMLNodeEdge() throws Exception {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        File output = new File(directory, "importNodeEdges.graphml");
        FileWriter fw = new FileWriter(output);
        fw.write(EXPECTED_READ_NODE_EDGE); fw.close();
        final String query = "CALL apoc.import.graphml($file,{readLabels:true})";
        final String absolutePath = output.getAbsolutePath();
        commonAssertionImportNodeEdge(absolutePath, query, map("file", absolutePath));
    }

    @Test
    public void testImportGraphMLNodeEdgeWithBinary() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        
        commonAssertionImportNodeEdge(null, "CALL apoc.import.graphml($file,{readLabels:true, compression: 'DEFLATE'})",
                map("file", fileToBinary(new File(directory, "importNodeEdges.graphml"), "DEFLATE")));
    }

    private void commonAssertionImportNodeEdge(String isBinary, String query, Map<String, Object> config) {
        TestUtil.testCall(db, query, config,
                (r) -> {
                    assertEquals(3L, r.get("nodes"));
                    assertEquals(3L, r.get("relationships"));
                    assertEquals(5L, r.get("properties"));
                    assertEquals(isBinary, r.get("file"));
                    assertEquals(isBinary == null ? "binary" : "file", r.get("source"));
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
            assertEquals(Arrays.asList(label("QWERTY")), ((Node)r.get("qwerty")).getLabels());
            assertEquals(Util.map("name", "qwerty"), ((Node)r.get("qwerty")).getAllProperties());
            assertEquals("KNOWS", ((Relationship)r.get("knows")).getType().name());
        });

    }

    private void assertBar(Node node){
        assertEquals(Arrays.asList(label("BAR")), node.getLabels());
        assertEquals(Util.map("name", "bar", "kids", "[a,b,c]"), node.getAllProperties());
    }

    private void assertFoo(Node node){
        assertEquals(Arrays.asList(label("FOO")), node.getLabels());
        assertEquals(Util.map("name", "foo"), node.getAllProperties());
    }

    @Test
    public void testExportDataGraphML() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        db.executeTransactionally("CREATE (p:Person {name: 'Foo'})");
        db.executeTransactionally("CREATE (p:Person {name: 'Bar'})");
        db.executeTransactionally("CREATE (p:Person {name: 'Baz'})");
        db.executeTransactionally("CREATE (p:Person {name: 'Foo0'})");

        File output = new File(directory, "data.graphml");
        TestUtil.testCall(db, "MATCH (person:Person) \n" +
                        "WHERE person.name STARTS WITH \"F\"\n" +
                        "WITH collect(person) as people\n" +
                        "CALL apoc.export.graphml.data(people, [], $file, {})\n" +
                        "YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data\n" +
                        "RETURN *",
                map("file", output.getAbsolutePath()),
                (r) -> assertEquals(2L, r.get("nodes")));
        assertXMLEquals(output, EXPECTED_DATA);
    }

    @Test
    public void testExportAllGraphML() {
        File output = new File(directory, "all.graphml");
        TestUtil.testCall(db, "CALL apoc.export.graphml.all($file,null)", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));
        assertXMLEquals(output, EXPECTED_FALSE);
    }

    @Test
    public void testExportAllGraphMLWithCompression() {
        final CompressionAlgo algo = CompressionAlgo.DEFLATE;
        File output = new File(directory, "all.graphml.zz");
        TestUtil.testCall(db, "CALL apoc.export.graphml.all($file, $config)",
                map("file", output.getAbsolutePath(), "config", map("compression", algo.name())),
                (r) -> assertResults(output, r, "database"));
        assertXMLEquals(BinaryTestUtil.readFileToString(output, StandardCharsets.UTF_8, algo), EXPECTED_FALSE);
    }
    
    @Test
    public void testGraphMlRoundtrip() {
        final CompressionAlgo algo = CompressionAlgo.NONE;
        File output = new File(directory, "all.graphml.zz");
        final Map<String, Object> params = map("file", output.getAbsolutePath(), 
                "config", map(CompressionConfig.COMPRESSION, algo.name(), "readLabels", true, "useTypes", true));
        TestUtil.testCall(db, "CALL apoc.export.graphml.all($file, $config)", params, (r) -> assertResults(output, r, "database"));

        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        TestUtil.testCall(db, "CALL apoc.import.graphml($file, $config) ", params,
                r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (n) RETURN n order by coalesce(n.name, '')", r -> {
            final ResourceIterator<Node> iterator = r.columnAs("n");
            final Node first = iterator.next();
            assertEquals(12L, first.getProperty("age"));
            assertFalse(first.hasProperty("name"));
            assertEquals(List.of(label("Bar")), first.getLabels());

            final Node second = iterator.next();
            assertEquals(42L, second.getProperty("age"));
            assertEquals("bar", second.getProperty("name"));
            assertEquals(List.of(label("Bar")), second.getLabels());

            final Node third = iterator.next();
            assertFalse(third.hasProperty("age"));
            assertEquals("foo", third.getProperty("name"));
            assertEquals(Set.of(label("Foo"), label("Foo2"), label("Foo0")), Iterables.asSet(third.getLabels()));

            assertFalse(iterator.hasNext());
        });
        
    }

    @Test
    public void testExportGraphGraphML() {
        File output = new File(directory, "graph.graphml");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, $file,null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertXMLEquals(output, EXPECTED_FALSE);
    }


    @Test
    public void testExportGraphGraphMLTypes() {
        File output = new File(directory, "graph.graphml");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, $file,{useTypes:true}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertXMLEquals(output, EXPECTED_TYPES);
    }

    @Test(expected = QueryExecutionException.class)
    public void testExportGraphGraphMLTypesWithNoExportConfig() {
        File output = new File(directory, "all.graphml");
        try {
            TestUtil.testCall(db, "CALL apoc.export.graphml.all($file,null)", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "database"));
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals(EXPORT_TO_FILE_ERROR, except.getMessage());
            throw e;
        }
    }

    @Test
    public void testImportAndExport() throws Exception {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        File output = new File(directory, "graphEmpty.graphml");
        db.executeTransactionally("CREATE (n:Test {name : '', limit : 3}) RETURN n");
        TestUtil.testCall(db, "call apoc.export.graphml.all($file,{storeNodeIds:true, readLabels:true, useTypes:true, defaultRelationshipType:'RELATED'})",
                map("file", output.getAbsolutePath()),
                (r) -> assertResultEmpty(output, r));
        assertXMLEquals(output, EXPECTED_TYPES_EMPTY);

        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        File input = new File(directory, "importEmpty.graphml");
        FileWriter fw = new FileWriter(input);
        fw.write(EXPECTED_TYPES_EMPTY); fw.close();
        TestUtil.testCall(db, "CALL apoc.import.graphml($file,{readLabels:true})", map("file", input.getAbsolutePath()),
                (r) -> assertResultEmpty(input, r));

        TestUtil.testCall(db, "MATCH (n:Test) RETURN n.name as name, n.limit as limit", null, (r) -> {
                assertEquals(StringUtils.EMPTY, r.get("name"));
                assertEquals(3L, r.get("limit"));
            }
        );
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (f:Foo:Foo2:Foo0 {name:'foo'})-[:KNOWS]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12,values:[1,2,3]})");

    }

    private void assertResultEmpty(File output, Map<String, Object> r) {
        assertEquals(1L, r.get("nodes"));
        assertEquals(0L, r.get("relationships"));
        assertEquals(2L, r.get("properties"));
        assertEquals(output.getAbsolutePath(), r.get("file"));
        if (r.get("source").toString().contains(":"))
            assertEquals("database: nodes(1), rels(0)", r.get("source"));
        else
            assertEquals("file", r.get("source"));
        assertEquals("graphml", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) > 0);
    }

    @Test
    public void testExportGraphGraphMLQueryGephi() {
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$file,{useTypes:true, format: 'gephi'}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertXMLEquals(output, EXPECTED_TYPES_PATH);
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaption() {
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$file,{useTypes:true, format: 'gephi', caption: ['bar','name','foo']}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertXMLEquals(output, EXPECTED_TYPES_PATH_CAPTION);
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaptionWrong() {
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$file,{useTypes:true, format: 'gephi', caption: ['c','d','e']}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertXMLEquals(output, EXPECTED_TYPES_PATH_WRONG_CAPTION);
    }

    @Test
    public void testExportAllGraphMLTinker() {
        File output = new File(directory, "all.graphml");
        TestUtil.testCall(db, "CALL apoc.export.graphml.all($file, {format:'tinkerpop'})", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));
        assertXMLEquals(output, EXPECTED_TINKER);
    }

    @Test
    public void testExportGraphGraphMLQueryTinkerPop() {
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$file,{useTypes:true, format: 'tinkerpop'}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertXMLEquals(output, EXPECTED_TYPES_PATH_CAPTION_TINKER);
    }

    @Test
    public void testExportGraphGraphMLQueryTinkerPopWithArrayCaption() {
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$file,{useTypes:true, format: 'tinkerpop', caption: ['bar','name','foo']}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertXMLEquals(output, EXPECTED_TYPES_PATH_CAPTION_TINKER);
    }

    @Test
    public void testExportGraphGraphMLQueryTinkerPopWithArrayCaptionWrong() {
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$file,{useTypes:true, format: 'tinkerpop', caption: ['c','d','e']}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertXMLEquals(output, EXPECTED_TYPES_PATH_CAPTION_TINKER);
    }

    @Test(expected = QueryExecutionException.class)
    public void testExportGraphGraphMLQueryGephiWithStringCaption() {
        File output = new File(directory, "query.graphml");
        try {
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$file,{useTypes:true, format: 'gephi', caption: 'name'}) ", map("file", output.getAbsolutePath()),
                (r) -> {});
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals("Only array of Strings are allowed!", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testExportGraphmlQueryWithStringCaptionCamelCase() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (f:Foo:Foo2:Foo0 {firstName:'foo'})-[:KNOWS]->(b:Bar {name:'bar',ageNow:42}),(c:Bar {age:12,values:[1,2,3]})");
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$file,{useTypes:true, format: 'gephi'}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(3L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertXMLEquals(output, EXPECTED_TYPES_PATH_CAMEL_CASE);
    }

    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertCommons(r);
        assertEquals(output.getAbsolutePath(), r.get("file"));
        if (r.get("source").toString().contains(":"))
            assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        else
            assertEquals("file", r.get("source"));
        assertNull("data should be null", r.get("data"));
    }

    private void assertCommons(Map<String, Object> r) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(8L, r.get("properties"));
        assertEquals("graphml", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
    }

    private void assertStreamResults(Map<String, Object> r, final String source) {
        assertCommons(r);
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertNull("file should be null", r.get("file"));
        assertNotNull("data should be not null", r.get("data"));
    }

    @Test
    public void testExportAllGraphMLStream() {
        TestUtil.testCall(db, "CALL apoc.export.graphml.all(null, {stream: true})",
                (r) -> {
                    assertStreamResults(r, "database");
                    assertXMLEquals(r.get("data"), EXPECTED_FALSE);
                });
    }

    @Test
    public void testExportAllGraphMLStreamWithCompression() {
        final CompressionAlgo algo = CompressionAlgo.BZIP2;
        TestUtil.testCall(db, "CALL apoc.export.graphml.all(null, $config)",
                map("config", map("compression", algo.name(), "stream", true)),
                (r) -> { 
                    assertStreamResults(r, "database");
                    assertXMLEquals(getDecompressedData(algo, r.get("data")), EXPECTED_FALSE);
                });
    }
}
