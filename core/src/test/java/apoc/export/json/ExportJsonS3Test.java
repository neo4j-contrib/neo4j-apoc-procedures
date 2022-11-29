package apoc.export.json;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.s3.S3BaseTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.FileTestUtil.assertStreamEquals;
import static apoc.util.MapUtil.map;
import static apoc.util.s3.S3TestUtil.readS3FileToString;
import static org.junit.Assert.*;

public class ExportJsonS3Test extends S3BaseTest {
    private static File directoryExpected = new File("src/test/resources/exportJSON");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, ExportJson.class, Graphs.class);
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12})");
    }

    @Test
    public void testExportAllJson() throws Exception {
        String filename = "all.json";
        String s3Url = s3Container.getUrl(filename);

        TestUtil.testCall(db, "CALL apoc.export.json.all($s3,null)",
                map("s3", s3Url),
                (r) -> {
                    assertResults(s3Url, r, "database");
                }
        );
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportPointMapDatetimeJson() throws Exception {
        String filename = "mapPointDatetime.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "return {data: 1, value: {age: 12, name:'Mike', data: {number: [1,3,5], born: date('2018-10-29'), place: point({latitude: 13.1, longitude: 33.46789})}}} as map, " +
                "datetime('2015-06-24T12:50:35.556+0100') AS theDateTime, " +
                "localdatetime('2015185T19:32:24') AS theLocalDateTime," +
                "point({latitude: 13.1, longitude: 33.46789}) as point," +
                "date('+2015-W13-4') as date," +
                "time('125035.556+0100') as time," +
                "localTime('12:50:35.556') as localTime";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(7)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportListNode() throws Exception {
        String filename = "listNode.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH (u:User) RETURN COLLECT(u) as list";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportListRel() throws Exception {
        String filename = "listRel.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH (u:User)-[rel:KNOWS]->(u2:User) RETURN COLLECT(rel) as list";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportListPath() throws Exception {
        String filename = "listPath.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH p = (u:User)-[rel]->(u2:User) RETURN COLLECT(p) as list";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportMap() throws Exception {
        String filename = "MapNode.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH (u:User)-[r:KNOWS]->(d:User) RETURN u {.*}, d {.*}, r {.*}";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(3)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportMapPath() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Mike',age:78,male:true})-[:KNOWS {since: 1850}]->(b:User {name:'John',age:18}),(c:User {age:39})");
        String filename = "MapPath.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH path = (u:User)-[rel:KNOWS]->(u2:User) RETURN {key:path} as map, 'Kate' as name";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(2)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportMapRel() throws Exception {
        String filename = "MapRel.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel {.*}";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportMapComplex() throws Exception {
        String filename = "MapComplex.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "RETURN {value:1, data:[10,'car',null, point({ longitude: 56.7, latitude: 12.78 }), point({ longitude: 56.7, latitude: 12.78, height: 8 }), point({ x: 2.3, y: 4.5 }), point({ x: 2.3, y: 4.5, z: 2 }),date('2018-10-10'), datetime('2018-10-18T14:21:40.004Z'), localdatetime({ year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645 }), {x:1, y:[1,2,3,{age:10}]}]} as key";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportGraphJson() throws Exception {
        String filename = "graph.json";
        String s3Url = s3Container.getUrl(filename);

        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.json.graph(graph, $s3) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportQueryJson() throws Exception {
        String filename = "query.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportQueryNodesJson() throws Exception {
        String filename = "query_nodes.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH (u:User) return u";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportQueryTwoNodesJson() throws Exception {
        String filename = "query_two_nodes.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH (u:User{name:'Adam'}), (l:User{name:'Jim'}) return u, l";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(2)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportQueryNodesJsonParams() throws Exception {
        String filename = "query_nodes_param.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH (u:User) WHERE u.age > $age return u";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3,{params:{age:10}})",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportQueryNodesJsonCount() throws Exception {
        String filename = "query_nodes_count.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH (n) return count(n)";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportData() throws Exception {
        String filename = "data.json";
        String s3Url = s3Container.getUrl(filename);

        TestUtil.testCall(db, "MATCH (nod:User) " +
                        "MATCH ()-[reels:KNOWS]->() " +
                        "WITH collect(nod) as node, collect(reels) as rels "+
                        "CALL apoc.export.json.data(node, rels, $s3, null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url),
                (r) -> {
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportDataPath() throws Exception {
        String filename = "query_nodes_path.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH p = (u:User)-[rel]->(u2:User) return u, rel, u2, p, u.name";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportAllWithWriteNodePropertiesJson() throws Exception {
        String filename = "with_node_properties.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3,{writeNodeProperties:true})",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportAllWithDefaultWriteNodePropertiesJson() throws Exception {
        String filename = "with_node_properties.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel";

        // default value for writeNodeProperties is true
        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3,{})",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportAllWithoutWriteNodePropertiesJson() throws Exception {
        String filename = "without_node_properties.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3,{writeNodeProperties:false})",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }

    @Test
    public void testExportQueryOrderJson() throws Exception {
        db.executeTransactionally("CREATE (f:User12:User1:User0:User {name:'Alan'})");
        String filename = "query_node_labels.json";
        String s3Url = s3Container.getUrl(filename);
        String query = "MATCH (u:User) WHERE u.name='Alan' RETURN u";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$s3)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertStreamStringEquals(directoryExpected, filename, s3Url);
    }
    
    private void assertStreamStringEquals(File directoryExpected, String filename, String s3Url) {
        final String actual = readS3FileToString(s3Url);
        assertStreamEquals(directoryExpected, filename, actual);
    }

    private void assertResults(String filename, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(11L, r.get("properties"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals(filename, r.get("file"));
        assertEquals("json", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }
}
