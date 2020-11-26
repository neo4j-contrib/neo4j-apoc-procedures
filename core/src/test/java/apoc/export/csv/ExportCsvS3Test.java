package apoc.export.csv;

import apoc.ApocSettings;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.s3.S3TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Ignore;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static apoc.util.MapUtil.map;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@Ignore("To use this test, you need to set the S3 bucket and region to a valid endpoint " +
        "and have your access key and secret key setup in your environment.")
public class ExportCsvS3Test {
    private static String S3_BUCKET_NAME = null;

    private static final String EXPECTED_QUERY_NODES = String.format("\"u\"%n" +
            "\"{\"\"id\"\":0,\"\"labels\"\":[\"\"User\"\",\"\"User1\"\"],\"\"properties\"\":{\"\"name\"\":\"\"foo\"\",\"\"age\"\":42,\"\"male\"\":true,\"\"kids\"\":[\"\"a\"\",\"\"b\"\",\"\"c\"\"]}}\"%n" +
            "\"{\"\"id\"\":1,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"name\"\":\"\"bar\"\",\"\"age\"\":42}}\"%n" +
            "\"{\"\"id\"\":2,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"age\"\":12}}\"%n");
    private static final String EXPECTED_QUERY = String.format("\"u.age\",\"u.name\",\"u.male\",\"u.kids\",\"labels(u)\"%n" +
            "\"42\",\"foo\",\"true\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"[\"\"User1\"\",\"\"User\"\"]\"%n" +
            "\"42\",\"bar\",\"\",\"\",\"[\"\"User\"\"]\"%n" +
            "\"12\",\"\",\"\",\"\",\"[\"\"User\"\"]\"%n");
    private static final String EXPECTED_QUERY_WITHOUT_QUOTES = String.format("u.age,u.name,u.male,u.kids,labels(u)%n" +
            "42,foo,true,[\"a\",\"b\",\"c\"],[\"User1\",\"User\"]%n" +
            "42,bar,,,[\"User\"]%n" +
            "12,,,,[\"User\"]%n");
    private static final String EXPECTED_QUERY_QUOTES_NONE = String.format("a.name,a.city,a.street,labels(a)%n" +
            "Andrea,Milano,Via Garibaldi, 7,[\"Address1\",\"Address\"]%n" +
            "Bar Sport,,,[\"Address\"]%n" +
            ",,via Benni,[\"Address\"]%n");
    private static final String EXPECTED_QUERY_QUOTES_ALWAYS = String.format("\"a.name\",\"a.city\",\"a.street\",\"labels(a)\"%n" +
            "\"Andrea\",\"Milano\",\"Via Garibaldi, 7\",\"[\"\"Address1\"\",\"\"Address\"\"]\"%n" +
            "\"Bar Sport\",\"\",\"\",\"[\"\"Address\"\"]\"%n" +
            "\"\",\"\",\"via Benni\",\"[\"\"Address\"\"]\"%n");
    private static final String EXPECTED_QUERY_QUOTES_NEEDED = String.format("a.name,a.city,a.street,labels(a)%n" +
            "Andrea,Milano,\"Via Garibaldi, 7\",\"[\"Address1\",\"Address\"]\"%n" +
            "Bar Sport,,,\"[\"Address\"]\"%n" +
            ",,via Benni,\"[\"Address\"]\"%n");
    private static final String EXPECTED = String.format("\"_id\",\"_labels\",\"age\",\"city\",\"kids\",\"male\",\"name\",\"street\",\"_start\",\"_end\",\"_type\"%n" +
            "\"0\",\":User:User1\",\"42\",\"\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"true\",\"foo\",\"\",,,%n" +
            "\"1\",\":User\",\"42\",\"\",\"\",\"\",\"bar\",\"\",,,%n" +
            "\"2\",\":User\",\"12\",\"\",\"\",\"\",\"\",\"\",,,%n" +
            "\"3\",\":Address:Address1\",\"\",\"Milano\",\"\",\"\",\"Andrea\",\"Via Garibaldi, 7\",,,%n" +
            "\"4\",\":Address\",\"\",\"\",\"\",\"\",\"Bar Sport\",\"\",,,%n" +
            "\"5\",\":Address\",\"\",\"\",\"\",\"\",\"\",\"via Benni\",,,%n" +
            ",,,,,,,,\"0\",\"1\",\"KNOWS\"%n" +
            ",,,,,,,,\"3\",\"4\",\"NEXT_DELIVERY\"%n");

    private static final String EXPECTED_NONE_QUOTES = String.format("_id,_labels,age,city,kids,male,name,street,_start,_end,_type%n" +
            "0,:User:User1,42,,[\"a\",\"b\",\"c\"],true,foo,,,,%n" +
            "1,:User,42,,,,bar,,,,%n" +
            "2,:User,12,,,,,,,,%n" +
            "3,:Address:Address1,,Milano,,,Andrea,Via Garibaldi, 7,,,%n" +
            "4,:Address,,,,,Bar Sport,,,,%n" +
            "5,:Address,,,,,,via Benni,,,%n" +
            ",,,,,,,,0,1,KNOWS%n" +
            ",,,,,,,,3,4,NEXT_DELIVERY%n");
    private static final String EXPECTED_NEEDED_QUOTES = String.format("_id,_labels,age,city,kids,male,name,street,_start,_end,_type%n" +
            "0,:User:User1,42,,\"[\"a\",\"b\",\"c\"]\",true,foo,,,,%n" +
            "1,:User,42,,,,bar,,,,%n" +
            "2,:User,12,,,,,,,,%n" +
            "3,:Address:Address1,,Milano,,,Andrea,\"Via Garibaldi, 7\",,,%n" +
            "4,:Address,,,,,Bar Sport,,,,%n" +
            "5,:Address,,,,,,via Benni,,,%n" +
            ",,,,,,,,0,1,KNOWS%n" +
            ",,,,,,,,3,4,NEXT_DELIVERY%n");

    private static File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_export_file_enabled, true);

    private static String getEnvVar(String envVarKey) throws Exception {
        return Optional.ofNullable(System.getenv(envVarKey)).orElseThrow(
                () -> new Exception(String.format("%s is not set in the environment", envVarKey))
        );
    }

    @BeforeClass
    public static void setUp() throws Exception {
        if (S3_BUCKET_NAME == null) {
            S3_BUCKET_NAME = getEnvVar("S3_BUCKET_NAME");
        }
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);
        db.executeTransactionally("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
        db.executeTransactionally("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
    }

    @AfterClass
    public static void tearDown() {
    }

    private static String getS3Url(String key) {
        return String.format("s3://:@/%s/%s", S3_BUCKET_NAME, key);
    }

    private String readFile(String fileName) {
        return TestUtil.readFileToString(new File(directory, fileName));
    }

    private void verifyUpload(String s3Url, String fileName, String expected) throws IOException {
        S3TestUtil.readFile(s3Url, Paths.get(directory.toString(), fileName).toString());
        assertEquals(expected, readFile(fileName));
    }

    @Test
    public void testExportAllCsvS3() throws Exception {
        String fileName = "all.csv";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,null)",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload(s3Url, fileName, EXPECTED);
    }

    @Test
    public void testExportAllCsvS3WithQuotes() throws Exception {
        String fileName = "all.csv";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,{quotes: true})",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload(s3Url, fileName, EXPECTED);
    }

    @Test
    public void testExportAllCsvS3WithoutQuotes() throws Exception {
        String fileName = "all1.csv";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,{quotes: 'none'})",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload(s3Url, fileName, EXPECTED_NONE_QUOTES);
    }

    @Test
    public void testExportAllCsvS3NeededQuotes() throws Exception {
        String fileName = "all2.csv";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,{quotes: 'ifNeeded'})",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload(s3Url, fileName, EXPECTED_NEEDED_QUOTES);
    }

    @Test
    public void testExportGraphCsv() throws Exception {
        String fileName = "graph.csv";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.csv.graph(graph, $s3,{quotes: 'none'}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyUpload(s3Url, fileName, EXPECTED_NONE_QUOTES);
    }

    @Test
    public void testExportGraphCsvWithoutQuotes() throws Exception {
        String fileName = "graph1.csv";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.csv.graph(graph, $s3,null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyUpload(s3Url, fileName, EXPECTED);
    }

    @Test
    public void testExportQueryCsv() throws Exception {
        String fileName = "query.csv";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.csv.query($query,$s3,null)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        verifyUpload(s3Url, fileName, EXPECTED_QUERY);
    }

    @Test
    public void testExportQueryCsvWithoutQuotes() throws Exception {
        String fileName = "query1.csv";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.csv.query($query,$s3,{quotes: false})",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        verifyUpload(s3Url, fileName, EXPECTED_QUERY_WITHOUT_QUOTES);
    }

    @Test
    public void testExportQueryNodesCsv() throws Exception {
        String fileName = "query_nodes.csv";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (u:User) return u";
        TestUtil.testCall(db, "CALL apoc.export.csv.query($query,$s3,null)",
                map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        verifyUpload(s3Url, fileName, EXPECTED_QUERY_NODES);
    }

    @Test
    public void testExportQueryNodesCsvParams() throws Exception {
        String fileName = "query_nodes1.csv";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (u:User) WHERE u.age > $age return u";
        TestUtil.testCall(db, "CALL apoc.export.csv.query($query,$s3,{params:{age:10}})", map("s3", s3Url, "query", query),
                (r) -> {
                    assertTrue("Should s3 statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        verifyUpload(s3Url, fileName, EXPECTED_QUERY_NODES);
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
