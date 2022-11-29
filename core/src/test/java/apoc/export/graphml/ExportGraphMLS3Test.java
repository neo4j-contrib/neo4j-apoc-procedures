package apoc.export.graphml;

import apoc.util.TestUtil;
import apoc.util.s3.S3BaseTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.FileNotFoundException;
import java.util.Map;

import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_FALSE;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_PATH;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_PATH_CAMEL_CASE;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_PATH_CAPTION;
import static apoc.export.graphml.ExportGraphMLTestUtil.EXPECTED_TYPES_PATH_WRONG_CAPTION;
import static apoc.export.graphml.ExportGraphMLTestUtil.assertXMLEquals;
import static apoc.export.graphml.ExportGraphMLTestUtil.setUpGraphMl;
import static apoc.util.MapUtil.map;
import static apoc.util.s3.S3TestUtil.readS3FileToString;
import static org.junit.Assert.*;

public class ExportGraphMLS3Test extends S3BaseTest {

    @Rule
    public TestName testName = new TestName();

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        setUpGraphMl(db, testName);
    }

    @Test
    public void testExportAllGraphML() throws Exception {
        String fileName = "all.graphml";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.graphml.all($s3, null)",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        assertXmlFileEquals(EXPECTED_FALSE, s3Url);
    }

    @Test
    public void testExportGraphGraphML() throws Exception {
        String fileName = "graph.graphml";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, $s3, null) " +
                        "YIELD nodes, relationships, properties, file, source, format, time " +
                        "RETURN *",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        assertXmlFileEquals(EXPECTED_FALSE, s3Url);
    }

    @Test
    public void testExportGraphGraphMLTypes() throws Exception {
        String fileName = "graph.graphml";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, $s3,{useTypes:true}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        assertXmlFileEquals(EXPECTED_TYPES, s3Url);
    }

    @Test
    public void testExportGraphGraphMLQueryGephi() throws Exception {
        String fileName = "query.graphml";
        String s3Url = s3Container.getUrl(fileName);
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
        assertXmlFileEquals(EXPECTED_TYPES_PATH, s3Url);
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaption() throws Exception {
        String fileName = "query.graphml";
        String s3Url = s3Container.getUrl(fileName);
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
        assertXmlFileEquals(EXPECTED_TYPES_PATH_CAPTION, s3Url);
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaptionWrong() throws Exception {
        String fileName = "query.graphml";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',$s3,{useTypes:true, format: 'gephi', caption: ['c','d','e']}) ",
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
        assertXmlFileEquals(EXPECTED_TYPES_PATH_WRONG_CAPTION, s3Url);
    }

    @Test
    public void testExportGraphmlQueryWithStringCaptionCamelCase() throws FileNotFoundException, Exception {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (f:Foo:Foo2:Foo0 {firstName:'foo'})-[:KNOWS]->(b:Bar {name:'bar',ageNow:42}),(c:Bar {age:12,values:[1,2,3]})");
        String fileName = "query.graphml";
        String s3Url = s3Container.getUrl(fileName);
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
        assertXmlFileEquals(EXPECTED_TYPES_PATH_CAMEL_CASE, s3Url);
    }

    private void assertXmlFileEquals(String expected, String s3Url) {
        final String actual = readS3FileToString(s3Url);
        assertXMLEquals(expected, actual);
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
