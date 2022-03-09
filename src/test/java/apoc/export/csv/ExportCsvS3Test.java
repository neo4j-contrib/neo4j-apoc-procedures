package apoc.export.csv;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.s3.S3TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.isCI;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class ExportCsvS3Test {
    private static S3TestUtil s3TestUtil;
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

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);
        db.execute("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})").close();
        db.execute("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})").close();
    }

    @AfterClass
    public static void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    public void testExportAllCsvS3() throws Exception {
        String fileName = "all.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,null)",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED);
    }

    @Test
    public void testExportAllCsvS3WithQuotes() throws Exception {
        String fileName = "all.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,{quotes: true})",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED);
    }

    @Test
    public void testExportAllCsvS3WithoutQuotes() throws Exception {
        String fileName = "all1.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,{quotes: 'none'})",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED_NONE_QUOTES);
    }

    @Test
    public void testExportAllCsvS3NeededQuotes() throws Exception {
        String fileName = "all2.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,{quotes: 'ifNeeded'})",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED_NEEDED_QUOTES);
    }

    @Test
    public void testExportGraphCsv() throws Exception {
        String fileName = "graph.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.csv.graph(graph, $s3,{quotes: 'none'}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED_NONE_QUOTES);
    }

    @Test
    public void testExportGraphCsvWithoutQuotes() throws Exception {
        String fileName = "graph1.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.csv.graph(graph, $s3,null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED);
    }

    @Test
    public void testExportQueryCsv() throws Exception {
        String fileName = "query.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.csv.query($query,$s3,null)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED_QUERY);
    }

    @Test
    public void testExportQueryCsvWithoutQuotes() throws Exception {
        String fileName = "query1.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.csv.query($query,$s3,{quotes: false})",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED_QUERY_WITHOUT_QUOTES);
    }

    @Test
    public void testExportQueryNodesCsv() throws Exception {
        String fileName = "query_nodes.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (u:User) return u";
        TestUtil.testCall(db, "CALL apoc.export.csv.query($query,$s3,null)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED_QUERY_NODES);
    }

    @Test
    public void testExportQueryNodesCsvParams() throws Exception {
        String fileName = "query_nodes1.csv";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (u:User) WHERE u.age > $age return u";
        TestUtil.testCall(db, "CALL apoc.export.csv.query($query,$s3,{params:{age:10}})", map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should s3 statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        s3TestUtil.verifyUpload(directory, fileName, ExportCsvTest.EXPECTED_QUERY_NODES);
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertEquals(6L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(12L, r.get("properties"));
        assertEquals(source + ": nodes(6), rels(2)", r.get("source"));
        assertEquals(fileName, r.get("file"));
        assertEquals("csv", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

}
