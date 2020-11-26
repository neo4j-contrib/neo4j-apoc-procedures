package apoc.export.graphml;

import apoc.ApocSettings;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.s3.S3TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.rules.TestName;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.ElementSelector;
import org.xmlunit.util.Nodes;

import javax.xml.namespace.QName;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static apoc.ApocConfig.*;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;
import static org.xmlunit.diff.ElementSelectors.byName;

@Ignore("To use this test, you need to set the S3 bucket and region to a valid endpoint " +
        "and have your access key and secret key setup in your environment.")
public class ExportGraphMLS3Test {
    private static String S3_BUCKET_NAME = null;

    public static final List<String> ATTRIBUTES_CONTAINING_NODE_IDS = Arrays.asList("id", "source", "target");

    public static final String GRAPH = "<graph id=\"G\" edgedefault=\"directed\">%n";
    public static final String HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n" +
            "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">%n";
    public static final String KEY_TYPES_FALSE = "<key id=\"born\" for=\"node\" attr.name=\"born\"/>%n" +
            "<key id=\"values\" for=\"node\" attr.name=\"values\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\"/>%n" +
            "<key id=\"label\" for=\"node\" attr.name=\"label\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\"/>%n";
    public static final String KEY_TYPES = "<key id=\"born\" for=\"node\" attr.name=\"born\" attr.type=\"string\"/>%n" +
            "<key id=\"values\" for=\"node\" attr.name=\"values\" attr.type=\"string\" attr.list=\"long\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"label\" for=\"node\" attr.name=\"label\" attr.type=\"string\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\" attr.type=\"string\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"long\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>%n";
    public static final String KEY_TYPES_PATH = "<key id=\"born\" for=\"node\" attr.name=\"born\" attr.type=\"string\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"label\" for=\"node\" attr.name=\"label\" attr.type=\"string\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"node\" attr.name=\"TYPE\" attr.type=\"string\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"long\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"edge\" attr.name=\"TYPE\" attr.type=\"string\"/>%n";
    public static final String KEY_TYPES_CAMEL_CASE = "<key id=\"firstName\" for=\"node\" attr.name=\"firstName\" attr.type=\"string\"/>%n" +
            "<key id=\"ageNow\" for=\"node\" attr.name=\"ageNow\" attr.type=\"long\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"label\" for=\"node\" attr.name=\"label\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"node\" attr.name=\"TYPE\" attr.type=\"string\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"edge\" attr.name=\"TYPE\" attr.type=\"string\"/>%n";
    public static final String DATA = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"labels\">:Foo:Foo0:Foo2</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":56.7,\"longitude\":12.78,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"labels\">:Bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":56.7,\"longitude\":12.78,\"height\":null}</data></node>%n" +
            "<node id=\"n2\" labels=\":Bar\"><data key=\"labels\">:Bar</data><data key=\"age\">12</data><data key=\"values\">[1,2,3]</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data></edge>%n";
    public static final String DATA_CAMEL_CASE =
            "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">foo</data><data key=\"firstName\">foo</data></node>%n" +
                    "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">bar</data><data key=\"name\">bar</data><data key=\"ageNow\">42</data></node>%n" +
                    "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";

    public static final String FOOTER = "</graph>%n" +
            "</graphml>";

    public static final String DATA_PATH = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">foo</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":56.7,\"longitude\":12.78,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":56.7,\"longitude\":12.78,\"height\":null}</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";

    public static final String DATA_PATH_CAPTION = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">foo</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":56.7,\"longitude\":12.78,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":56.7,\"longitude\":12.78,\"height\":null}</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";

    public static final String DATA_PATH_CAPTION_DEFAULT = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">point({x: 56.7, y: 12.78, z: 100.0, crs: 'wgs-84-3d'})</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":56.7,\"longitude\":12.78,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">42</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":56.7,\"longitude\":12.78,\"height\":null}</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";

    private static final String EXPECTED_TYPES_PATH = String.format(HEADER + KEY_TYPES_PATH + GRAPH + DATA_PATH + FOOTER);
    private static final String EXPECTED_TYPES_PATH_CAPTION = String.format(HEADER + KEY_TYPES_PATH + GRAPH + DATA_PATH_CAPTION + FOOTER);
    private static final String EXPECTED_TYPES_PATH_WRONG_CAPTION = String.format(HEADER + KEY_TYPES_PATH + GRAPH + DATA_PATH_CAPTION_DEFAULT + FOOTER);
    private static final String EXPECTED_TYPES = String.format(HEADER + KEY_TYPES + GRAPH + DATA + FOOTER);
    private static final String EXPECTED_FALSE = String.format(HEADER + KEY_TYPES_FALSE + GRAPH + DATA + FOOTER);
    private static final String EXPECTED_TYPES_PATH_CAMEL_CASE = String.format(HEADER + KEY_TYPES_CAMEL_CASE + GRAPH + DATA_CAMEL_CASE + FOOTER);

    @Rule
    public TestName testName = new TestName();

    private static final String TEST_WITH_NO_IMPORT = "WithNoImportConfig";
    private static final String TEST_WITH_NO_EXPORT = "WithNoExportConfig";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_use__neo4j__config, false)
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    private static String getS3Url(String key) {
        return String.format("s3://:@/%s/%s", S3_BUCKET_NAME, key);
    }

    private static String readFile(String fileName) {
        return TestUtil.readFileToString(new File(directory, fileName));
    }

    private  void verifyUpload(String s3Url, String fileName, String expected) throws IOException {
        S3TestUtil.readFile(s3Url, Paths.get(directory.toString(), fileName).toString());
        assertXMLEquals(expected, readFile(fileName));
    }

    private static String getEnvVar(String envVarKey) throws Exception {
        return Optional.ofNullable(System.getenv(envVarKey)).orElseThrow(
                () -> new Exception(String.format("%s is not set in the environment", envVarKey))
            );
    }

    @Before
    public void setUp() throws Exception {
        if (S3_BUCKET_NAME == null) {
            S3_BUCKET_NAME = getEnvVar("S3_BUCKET_NAME");
        }

        TestUtil.registerProcedure(db, ExportGraphML.class, Graphs.class);

        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, Boolean.toString(!testName.getMethodName().endsWith(TEST_WITH_NO_EXPORT)));
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, Boolean.toString(!testName.getMethodName().endsWith(TEST_WITH_NO_IMPORT)));

        db.executeTransactionally("CREATE (f:Foo:Foo2:Foo0 {name:'foo', born:Date('2018-10-10'), place:point({ longitude: 56.7, latitude: 12.78, height: 100 })})-[:KNOWS]->(b:Bar {name:'bar',age:42, place:point({ longitude: 56.7, latitude: 12.78})}),(c:Bar {age:12,values:[1,2,3]})");
    }

    @Test
    public void testExportAllGraphML() throws Exception {
        String fileName = "all.graphml";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.graphml.all($s3, null)",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload(s3Url, fileName, EXPECTED_FALSE);
    }

    @Test
    public void testExportGraphGraphML() throws Exception {
        String fileName = "graph.graphml";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, $s3, null) " +
                        "YIELD nodes, relationships, properties, file, source, format, time " +
                        "RETURN *",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyUpload(s3Url, fileName, EXPECTED_FALSE);
    }

    private void assertXMLEquals(Object output, String xmlString) {
        Diff myDiff = DiffBuilder.compare(xmlString)
                .withTest(output)
                .checkForSimilar()
                .ignoreWhitespace()
                .withAttributeFilter(attr -> !ATTRIBUTES_CONTAINING_NODE_IDS.contains(attr.getLocalName())) // ignore id properties
                .withNodeMatcher(new DefaultNodeMatcher((ElementSelector) (controlElement, testElement) -> {
                    if (!byName.canBeCompared(controlElement, testElement)) {
                        return false;
                    }
                    Map<QName, String> cAttrs = Nodes.getAttributes(controlElement);
                    Map<QName, String> tAttrs = Nodes.getAttributes(testElement);
                    if (cAttrs.size() != tAttrs.size()) {
                        return false;
                    }
                    for (Map.Entry<QName, String> e: cAttrs.entrySet()) {
                        if ((!ATTRIBUTES_CONTAINING_NODE_IDS.contains(e.getKey().getLocalPart()))
                                && (!e.getValue().equals(tAttrs.get(e.getKey())))) {
                            return false;
                        }
                    }
                    return true;
                }))
                .build();

        assertFalse(myDiff.toString(), myDiff.hasDifferences());
    }

    @Test
    public void testExportGraphGraphMLTypes() throws Exception {
        String fileName = "graph.graphml";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, $s3,{useTypes:true}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyUpload(s3Url, fileName, EXPECTED_TYPES);
    }

    @Test
    public void testExportGraphGraphMLQueryGephi() throws Exception {
        String fileName = "query.graphml";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$s3,{useTypes:true, format: 'gephi'}) ",
                map("s3", s3Url),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(s3Url, r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        verifyUpload(s3Url, fileName, EXPECTED_TYPES_PATH);
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaption() throws Exception {
        String fileName = "query.graphml";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$s3,{useTypes:true, format: 'gephi', caption: ['bar','name','foo']}) ",
                map("s3", s3Url),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(s3Url, r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        verifyUpload(s3Url, fileName, EXPECTED_TYPES_PATH_CAPTION);
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaptionWrong() throws Exception {
        String fileName = "query.graphml";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$s3,{useTypes:true, format: 'gephi', caption: ['a','b','c']}) ",
                map("s3", s3Url),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(s3Url, r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        verifyUpload(s3Url, fileName, EXPECTED_TYPES_PATH_WRONG_CAPTION);
    }

    @Test
    public void testExportGraphmlQueryWithStringCaptionCamelCase() throws FileNotFoundException, Exception {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (f:Foo:Foo2:Foo0 {firstName:'foo'})-[:KNOWS]->(b:Bar {name:'bar',ageNow:42}),(c:Bar {age:12,values:[1,2,3]})");
        String fileName = "query.graphml";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$s3,{useTypes:true, format: 'gephi'}) ",
                map("s3", s3Url),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(3L, r.get("properties"));
                    assertEquals(s3Url, r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        verifyUpload(s3Url, fileName, EXPECTED_TYPES_PATH_CAMEL_CASE);
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertCommons(r);
        assertEquals(fileName, r.get("file"));
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

}
