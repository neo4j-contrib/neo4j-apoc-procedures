package apoc.export.graphml;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.s3.S3TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
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

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.isCI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.xmlunit.diff.ElementSelectors.byName;

public class ExportGraphMLS3Test {
    private static S3TestUtil s3TestUtil;
    public static final List<String> ATTRIBUTES_CONTAINING_NODE_IDS = Arrays.asList("id", "source", "target");

    @Rule
    public TestName testName = new TestName();

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @BeforeClass
    public static void startS3TestUtil() {
        assumeFalse(isCI());
        s3TestUtil = new S3TestUtil();
    }

    @AfterClass
    public static void stopS3TestUtil() {
        if (s3TestUtil != null && s3TestUtil.isRunning()) {
            s3TestUtil.close();
        }
    }

    @Before
    public void setUp() throws Exception {
        GraphDatabaseBuilder builder  = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.use_neo4j_config", "false")
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath());
        if (!testName.getMethodName().endsWith(ExportGraphMLTest.TEST_WITH_NO_EXPORT)) {
            builder.setConfig("apoc.export.file.enabled", "true");
        }
        if (!testName.getMethodName().endsWith(ExportGraphMLTest.TEST_WITH_NO_IMPORT)) {
            builder.setConfig("apoc.import.file.enabled", "true");
        }
        db = builder.newGraphDatabase();
        TestUtil.registerProcedure(db, ExportGraphML.class, Graphs.class);
        db.execute("CREATE (f:Foo:Foo2:Foo0 {name:'foo', born:Date('2018-10-10'), place:point({ longitude: 56.7, latitude: 12.78, height: 100 })})-[:KNOWS]->(b:Bar {name:'bar',age:42, place:point({ longitude: 56.7, latitude: 12.78})}),(c:Bar {age:12,values:[1,2,3]})").close();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExportAllGraphML() throws Exception {
        String fileName = "all.graphml";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.graphml.all($s3, null)",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        verifyXmlUpload(s3Url, fileName, ExportGraphMLTest.EXPECTED_FALSE);
    }

    @Test
    public void testExportGraphGraphML() throws Exception {
        String fileName = "graph.graphml";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, $s3, null) " +
                        "YIELD nodes, relationships, properties, file, source, format, time " +
                        "RETURN *",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyXmlUpload(s3Url, fileName, ExportGraphMLTest.EXPECTED_FALSE);
    }

    @Test
    public void testExportGraphGraphMLTypes() throws Exception {
        String fileName = "graph.graphml";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, $s3,{useTypes:true}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyXmlUpload(s3Url, fileName, ExportGraphMLTest.EXPECTED_TYPES);
    }

    @Test
    public void testExportGraphGraphMLQueryGephi() throws Exception {
        String fileName = "query.graphml";
        String s3Url = s3TestUtil.getUrl(fileName);
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
        verifyXmlUpload(s3Url, fileName, ExportGraphMLTest.EXPECTED_TYPES_PATH);
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaption() throws Exception {
        String fileName = "query.graphml";
        String s3Url = s3TestUtil.getUrl(fileName);
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
        verifyXmlUpload(s3Url, fileName, ExportGraphMLTest.EXPECTED_TYPES_PATH_CAPTION);
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaptionWrong() throws Exception {
        String fileName = "query.graphml";
        String s3Url = s3TestUtil.getUrl(fileName);
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
        verifyXmlUpload(s3Url, fileName, ExportGraphMLTest.EXPECTED_TYPES_PATH_WRONG_CAPTION);
    }

    @Test
    public void testExportGraphmlQueryWithStringCaptionCamelCase() throws FileNotFoundException, Exception {
        db.execute("MATCH (n) detach delete (n)");
        db.execute("CREATE (f:Foo:Foo2:Foo0 {firstName:'foo'})-[:KNOWS]->(b:Bar {name:'bar',ageNow:42}),(c:Bar {age:12,values:[1,2,3]})");
        String fileName = "query.graphml";
        String s3Url = s3TestUtil.getUrl(fileName);
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
        verifyXmlUpload(s3Url, fileName, ExportGraphMLTest.EXPECTED_TYPES_PATH_CAMEL_CASE);
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

    private void verifyXmlUpload(String s3Url, String fileName, String expected) throws IOException {
        S3TestUtil.readFile(s3Url, Paths.get(directory.toString(), fileName).toString());
        assertXMLEquals(expected, readFile(fileName));
    }

    private static String readFile(String fileName) {
        return TestUtil.readFileToString(new File(directory, fileName));
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
}
