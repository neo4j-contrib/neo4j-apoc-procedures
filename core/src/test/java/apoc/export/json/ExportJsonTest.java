package apoc.export.json;

import apoc.graph.Graphs;
import apoc.util.BinaryTestUtil;
import apoc.util.CompressionAlgo;
import apoc.util.FileTestUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.BinaryTestUtil.getDecompressedData;
import static apoc.util.CompressionAlgo.DEFLATE;
import static apoc.util.CompressionAlgo.FRAMED_SNAPPY;
import static apoc.util.CompressionAlgo.NONE;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static apoc.export.json.JsonFormat.Format;

public class ExportJsonTest {

    private static final String DEFLATE_EXT = ".zz";
    private static File directory = new File("target/import");
    private static File directoryExpected = new File("src/test/resources/exportJSON");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
        .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, ExportJson.class, ImportJson.class, Graphs.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12})");
    }

    @Test
    public void testExportAllJson() {
        String filename = "all.json";
        TestUtil.testCall(db, "CALL apoc.export.json.all($file,null)",
                map("file", filename),
                (r) -> {
                    assertResults(filename, r, "database");
                }
        );
        assertFileEquals(filename);
    }

    @Test
    public void testJsonRoundtrip() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:User) REQUIRE n.neo4jImportId IS UNIQUE;");
        String filename = "all.json.gzip";
        final Map<String, Object> params = map("file", filename, "config", map(COMPRESSION, CompressionAlgo.GZIP.name()));
        TestUtil.testCall(db, "CALL apoc.export.json.all($file, $config)", params,
                (r) -> assertResults(filename, r, "database")
        );
        
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        TestUtil.testCall(db, "CALL apoc.import.json($file, $config) ", params,
                r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (n) RETURN n order by coalesce(n.name, '')", r -> {
            final ResourceIterator<Node> iterator = r.columnAs("n");
            final Node first = iterator.next();
            assertEquals(12L, first.getProperty("age"));
            assertFalse(first.hasProperty("name"));
            assertEquals(List.of(Label.label("User")), first.getLabels());
            
            final Node second = iterator.next();
            assertEquals(42L, second.getProperty("age"));
            assertEquals("Adam", second.getProperty("name"));
            assertEquals(List.of(Label.label("User")), second.getLabels());
            
            final Node third = iterator.next();
            assertEquals(42L, third.getProperty("age"));
            assertEquals("Jim", third.getProperty("name"));
            assertEquals(List.of(Label.label("User")), third.getLabels());
            
            assertFalse(iterator.hasNext());
        });

    }

    @Test
    public void testExportAllJsonArray() {
        String filename = "all_array.json";
        TestUtil.testCall(db, "CALL apoc.export.json.all($file, {jsonFormat: 'ARRAY_JSON'})",
                map("file", filename),
                (r) -> assertResults(filename, r, "database"));
        assertFileEquals(filename);
    }

    @Test
    public void testExportAllJsonFields() {
        String filename = "all_fields.json";
        TestUtil.testCall(db, "CALL apoc.export.json.all($file, {jsonFormat: 'JSON'})",
                map("file", filename),
                (r) -> assertResults(filename, r, "database"));
        assertFileEquals(filename);
    }

    @Test
    public void testExportAllJsonIdAsKeys() {
        String filename = "all_id_as_keys.json";
        TestUtil.testCall(db, "CALL apoc.export.json.all($file, {jsonFormat: 'JSON_ID_AS_KEYS'})",
                map("file", filename),
                (r) -> assertResults(filename, r, "database"));
        assertFileEquals(filename);
    }

    @Test
    public void testExportAllJsonStream() {
        String filename = "all.json";
        TestUtil.testCall(db, "CALL apoc.export.json.all(null, {stream: true})",
                (r) -> {
                    assertStreamResults(r, "database");
                    assertStreamEquals(filename, r.get("data").toString());
                }
        );
    }

    @Test
    public void testExportAllJsonStreamWithFormatConfig() {
        Map.of(Format.JSON_LINES.name(), "all.json", 
                Format.JSON.name(), "all_fields.json",
                Format.ARRAY_JSON.name(), "all_array.json",
                Format.JSON_ID_AS_KEYS.name(), "all_id_as_keys.json").forEach((jsonFormat, fileName) -> {

            TestUtil.testCall(db, "CALL apoc.export.json.all(null, $config)",
                    map("config", map("jsonFormat", jsonFormat, "stream", true)),
                    (r) -> {
                        assertStreamResults(r, "database");
                        assertStreamEquals(fileName, r.get("data").toString());
                    }
            );   
        });
    }

    @Test
    public void testExportAllJsonStreamWithCompression() {
        final CompressionAlgo algo = FRAMED_SNAPPY;
        String expectedFile = "all.json";
        String filename = expectedFile + ".sz";
        TestUtil.testCall(db, "CALL apoc.export.json.all(null, $config)",
                map("file", filename, "config", map("stream", true, "compression", algo.name())),
                (r) -> {
                    assertStreamResults(r, "database");
                    assertStreamEquals(expectedFile, getDecompressedData(algo, r.get("data")));
                }
        );
    }

    @Test
    public void testExportPointMapDatetimeJson() {
        String filename = "mapPointDatetime.json";
        String query = "return {data: 1, value: {age: 12, name:'Mike', data: {number: [1,3,5], born: date('2018-10-29'), place: point({latitude: 13.1, longitude: 33.46789})}}} as map, " +
                "datetime('2015-06-24T12:50:35.556+0100') AS theDateTime, " +
                "localdatetime('2015185T19:32:24') AS theLocalDateTime," +
                "point({latitude: 13.1, longitude: 33.46789}) as point," +
                "date('+2015-W13-4') as date," +
                "time('125035.556+0100') as time," +
                "localTime('12:50:35.556') as localTime";
        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(7)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                    assertFileEquals(filename);
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportPointMapDatetimeStreamJson() {
        String filename = "mapPointDatetime.json";
        String query = "return {data: 1, value: {age: 12, name:'Mike', data: {number: [1,3,5], born: date('2018-10-29'), place: point({latitude: 13.1, longitude: 33.46789})}}} as map, " +
                "datetime('2015-06-24T12:50:35.556+0100') AS theDateTime, " +
                "localdatetime('2015185T19:32:24') AS theLocalDateTime," +
                "point({latitude: 13.1, longitude: 33.46789}) as point," +
                "date('+2015-W13-4') as date," +
                "time('125035.556+0100') as time," +
                "localTime('12:50:35.556') as localTime";
        TestUtil.testCall(db, "CALL apoc.export.json.query($query, null, {stream: true})",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement", r.get("source").toString().contains("statement: cols(7)"));
                    assertStreamEquals(filename, r.get("data").toString());
                });
    }

    @Test
    public void testExportListNode() {
        String filename = "listNode.json";

        String query = "MATCH (u:User) RETURN COLLECT(u) as list";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename, "query", query),
                (r) -> assertionsListNode(filename, r));
        assertFileEquals(filename);
    }

    @Test
    public void testExportListNodeWithCompression() {
        String query = "MATCH (u:User) RETURN COLLECT(u) as list";
        final CompressionAlgo algo = DEFLATE;
        String expectedFile = "listNode.json";
        String filename = expectedFile + DEFLATE_EXT;

        TestUtil.testCall(db, "CALL apoc.export.json.query($query, $file, $config)",
                map("file", filename, "query", query, "config", map("compression", algo.name())),
                (r) -> assertionsListNode(filename, r));
        assertFileEquals(expectedFile, algo);
    }

    private void assertionsListNode(String filename, Map<String, Object> r) {
        assertTrue("Should get statement", r.get("source").toString().contains("statement: cols(1)"));
        assertEquals(filename, r.get("file"));
        assertEquals("json", r.get("format"));
    }
    
    @Test
    public void testExportListRel() {
        String filename = "listRel.json";

        String query = "MATCH (u:User)-[rel:KNOWS]->(u2:User) RETURN COLLECT(rel) as list";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)", map("file", filename,"query",query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportListPath() {
        String filename = "listPath.json";

        String query = "MATCH p = (u:User)-[rel]->(u2:User) RETURN COLLECT(p) as list";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportMap() {
        String filename = "MapNode.json";

        String query = "MATCH (u:User)-[r:KNOWS]->(d:User) RETURN u {.*}, d {.*}, r {.*}";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(3)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportMapPath() {
        db.executeTransactionally("CREATE (f:User {name:'Mike',age:78,male:true})-[:KNOWS {since: 1850}]->(b:User {name:'John',age:18}),(c:User {age:39})");
        String filename = "MapPath.json";

        String query = "MATCH path = (u:User)-[rel:KNOWS]->(u2:User) RETURN {key:path} as map, 'Kate' as name";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(2)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportMapRel() {
        String filename = "MapRel.json";

        String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel {.*}";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename,"query",query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportMapComplex() {
        String filename = "MapComplex.json";

        String query = "RETURN {value:1, data:[10,'car',null, point({ longitude: 56.7, latitude: 12.78 }), point({ longitude: 56.7, latitude: 12.78, height: 8 }), point({ x: 2.3, y: 4.5 }), point({ x: 2.3, y: 4.5, z: 2 }),date('2018-10-10'), datetime('2018-10-18T14:21:40.004Z'), localdatetime({ year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645 }), {x:1, y:[1,2,3,{age:10}]}]} as key";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportGraphJson() {
        String filename = "graph.json";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.json.graph(graph, $file) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", filename),
                (r) -> assertResults(filename, r, "graph"));
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryJson() {
        String filename = "query.json";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryNodesJson() {
        String filename = "query_nodes.json";
        String query = "MATCH (u:User) return u";
        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename,"query",query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryTwoNodesJson() {
        String filename = "query_two_nodes.json";
        String query = "MATCH (u:User{name:'Adam'}), (l:User{name:'Jim'}) return u, l";
        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(2)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });

        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryNodesJsonParams() {
        String filename = "query_nodes_param.json";
        String query = "MATCH (u:User) WHERE u.age > $age return u";
        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file,{params:{age:10}})",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryNodesJsonCount() {
        String filename = "query_nodes_count.json";
        String query = "MATCH (n) return count(n)";
        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportData() {
        String filename = "data.json";
        TestUtil.testCall(db, "MATCH (nod:User) " +
                        "MATCH ()-[reels:KNOWS]->() " +
                        "WITH collect(nod) as node, collect(reels) as rels "+
                        "CALL apoc.export.json.data(node, rels, $file, null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("file", filename),
                (r) -> {
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportDataPath() {
        String filename = "query_nodes_path.json";
        String query = "MATCH p = (u:User)-[rel]->(u2:User) return u, rel, u2, p, u.name";
        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)",
                map("file", filename, "query", query),
                (r) -> {
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportAllWithWriteNodePropertiesJson() {
        String filename = "with_node_properties.json";
        String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file,{writeNodeProperties:true})",
                map("file", filename,"query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportAllWithDefaultWriteNodePropertiesJson() {
        String filename = "with_node_properties.json";
        String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel";

        // default value for writeNodeProperties is true
        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file,{})",
                map("file", filename,"query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportAllWithoutWriteNodePropertiesJson() {
        String filename = "without_node_properties.json";
        String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file,{writeNodeProperties:false})",
                map("file", filename,"query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryOrderJson() {
        db.executeTransactionally("CREATE (f:User12:User1:User0:User {name:'Alan'})");
        String filename = "query_node_labels.json";
        String query = "MATCH (u:User) WHERE u.name='Alan' RETURN u";

        TestUtil.testCall(db, "CALL apoc.export.json.query($query,$file)", map("file", filename,"query",query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }
    
    @Test
    public void testExportWgsPoint() {
        db.executeTransactionally("CREATE (p:Position {place: point({latitude: 12.78, longitude: 56.7, height: 1.1})})");

        TestUtil.testCall(db, "CALL apoc.export.json.query($query, null, {stream: true}) YIELD data RETURN data",
                map("query", "MATCH (p:Position) RETURN p.place as place"),
                (r) -> {
                    String data = (String) r.get("data");
                    Map<String, Object> map = Util.fromJson(data, Map.class);
                    Map<String, Object> place = (Map<String, Object>) map.get("place");
                    assertEquals(12.78D, (double) place.get("latitude"), 0);
                    assertEquals(56.7D, (double) place.get("longitude"), 0);
                    assertEquals(1.1D, (double) place.get("height"), 0);
                });
        
        db.executeTransactionally("MATCH (n:Position) DETACH DELETE n");
        
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

    private void assertFileEquals(String fileName, CompressionAlgo algo) {
        String fileExt = algo.equals(DEFLATE) ? DEFLATE_EXT : "";
        String actualText = BinaryTestUtil.readFileToString(new File(directory, fileName + fileExt), UTF_8, algo);
        assertStreamEquals(fileName, actualText);
    }

    private void assertFileEquals(String fileName) {
        assertFileEquals(fileName, NONE);
    }

    private void assertStreamResults(Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(11L, r.get("properties"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertNull("file should be null", r.get("file"));
        assertNotNull("data should be not null", r.get("data"));
        assertEquals("json", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    private void assertStreamEquals(String fileName, String actualText) {
        FileTestUtil.assertStreamEquals(directoryExpected, fileName, actualText);
    }
}
