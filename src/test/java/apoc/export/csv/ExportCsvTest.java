package apoc.export.csv;

import apoc.graph.Graphs;
import apoc.util.HdfsTestUtils;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Consumer;

import static apoc.util.MapUtil.map;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCsvTest {

    private static final String EXPECTED_QUERY_NODES = String.format("\"u\"%n" +
            "\"{\"\"id\"\":0,\"\"labels\"\":[\"\"User\"\",\"\"User1\"\"],\"\"properties\"\":{\"\"name\"\":\"\"foo\"\",\"\"age\"\":42,\"\"male\"\":true,\"\"kids\"\":[\"\"a\"\",\"\"b\"\",\"\"c\"\"]}}\"%n" +
            "\"{\"\"id\"\":1,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"name\"\":\"\"bar\"\",\"\"age\"\":42}}\"%n" +
            "\"{\"\"id\"\":2,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"age\"\":12}}\"");
    private static final String EXPECTED_QUERY = String.format("\"u.age\",\"u.name\",\"u.male\",\"u.kids\",\"labels(u)\"%n" +
            "\"42\",\"foo\",\"true\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"[\"\"User1\"\",\"\"User\"\"]\"%n" +
            "\"42\",\"bar\",\"\",\"\",\"[\"\"User\"\"]\"%n" +
            "\"12\",\"\",\"\",\"\",\"[\"\"User\"\"]\"");
    private static final String EXPECTED_QUERY_WITHOUT_QUOTES = String.format("u.age,u.name,u.male,u.kids,labels(u)%n" +
            "42,foo,true,[\"a\",\"b\",\"c\"],[\"User1\",\"User\"]%n" +
            "42,bar,,,[\"User\"]%n" +
            "12,,,,[\"User\"]");
    private static final String EXPECTED_QUERY_QUOTES_NONE= String.format( "a.name,a.city,a.street,labels(a)%n" +
            "Andrea,Milano,Via Garibaldi, 7,[\"Address1\",\"Address\"]%n" +
            "Bar Sport,,,[\"Address\"]%n" +
            ",,via Benni,[\"Address\"]" );
    private static final String EXPECTED_QUERY_QUOTES_ALWAYS= String.format( "\"a.name\",\"a.city\",\"a.street\",\"labels(a)\"%n" +
            "\"Andrea\",\"Milano\",\"Via Garibaldi, 7\",\"[\"\"Address1\"\",\"\"Address\"\"]\"%n" +
            "\"Bar Sport\",\"\",\"\",\"[\"\"Address\"\"]\"%n" +
            "\"\",\"\",\"via Benni\",\"[\"\"Address\"\"]\"");
    private static final String EXPECTED_QUERY_QUOTES_NEEDED= String.format( "a.name,a.city,a.street,labels(a)%n" +
            "Andrea,Milano,\"Via Garibaldi, 7\",\"[\"Address1\",\"Address\"]\"%n" +
            "Bar Sport,,,\"[\"Address\"]\"%n" +
            ",,via Benni,\"[\"Address\"]\"");
    private static final String EXPECTED = String.format("\"_id\",\"_labels\",\"name\",\"age\",\"male\",\"kids\",\"street\",\"city\",\"_start\",\"_end\",\"_type\"%n" +
            "\"0\",\":User:User1\",\"foo\",\"42\",\"true\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"\",\"\",,,%n" +
            "\"1\",\":User\",\"bar\",\"42\",\"\",\"\",\"\",\"\",,,%n" +
            "\"2\",\":User\",\"\",\"12\",\"\",\"\",\"\",\"\",,,%n" +
            "\"20\",\":Address:Address1\",\"Andrea\",\"\",\"\",\"\",\"Via Garibaldi, 7\",\"Milano\",,,%n" +
            "\"21\",\":Address\",\"Bar Sport\",\"\",\"\",\"\",\"\",\"\",,,%n" +
            "\"22\",\":Address\",\"\",\"\",\"\",\"\",\"via Benni\",\"\",,,%n" +
            ",,,,,,,,\"0\",\"1\",\"KNOWS\"%n" +
            ",,,,,,,,\"20\",\"21\",\"NEXT_DELIVERY\"");
    private static final String EXPECTED_NONE_QUOTES = String.format("_id,_labels,name,age,male,kids,street,city,_start,_end,_type%n" +
            "0,:User:User1,foo,42,true,[\"a\",\"b\",\"c\"],,,,,%n" +
            "1,:User,bar,42,,,,,,,%n" +
            "2,:User,,12,,,,,,,%n" +
            "20,:Address:Address1,Andrea,,,,Via Garibaldi, 7,Milano,,,%n" +
            "21,:Address,Bar Sport,,,,,,,,%n" +
            "22,:Address,,,,,via Benni,,,,%n" +
            ",,,,,,,,0,1,KNOWS%n" +
            ",,,,,,,,20,21,NEXT_DELIVERY");
    private static final String EXPECTED_NEEDED_QUOTES = String.format("_id,_labels,name,age,male,kids,street,city,_start,_end,_type%n" +
            "0,:User:User1,foo,42,true,\"[\"a\",\"b\",\"c\"]\",,,,,%n" +
            "1,:User,bar,42,,,,,,,%n" +
            "2,:User,,12,,,,,,,%n" +
            "20,:Address:Address1,Andrea,,,,\"Via Garibaldi, 7\",Milano,,,%n" +
            "21,:Address,Bar Sport,,,,,,,,%n" +
            "22,:Address,,,,,via Benni,,,,%n" +
            ",,,,,,,,0,1,KNOWS%n" +
            ",,,,,,,,20,21,NEXT_DELIVERY");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_MOVIE = String.format(
            "id:ID,year:long,movieId,title,:LABEL%n" +
                    "3,1999,tt0133093,The Matrix,Movie");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_TEST_NODE = String.format(
            "id:ID,born_2D:point,time:time,localtime:localtime,dateTime:datetime,localDateTime:localdatetime,date:date,born_3D:point,duration:duration,:LABEL%n" +
                    "10,\"{crs:cartesian,x:2.3,y:4.5}\",12:50:35.556+01:00,12:50:35.556,2018-10-30T12:50:35.556+01:00,2018-10-30T19:32:24,2018-10-30,\"{crs:wgs-84-3d,latitude:56.7,longitude:12.78,height:100.0}\",P5M1DT12H,Test");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_PRODUCT = String.format(
            "id:ID,reorderLevel:long,unitsOnOrder:long,quantityPerUnit,unitPrice:double,categoryID:long,discontinued:boolean,productName,supplierID:long,productID:long,unitsInStock:long,:LABEL%n" +
                    "9,10,0,10 boxes x 20 bags,18,1,false,Chai,1,1,39,Product");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_MOVIE_SEQUEL = String.format(
            "id:ID,title,movieId,year:long,:LABEL%n" +
                    "4,The Matrix Reloaded,tt0234215,2003,Movie;Sequel%n" +
                    "5,The Matrix Revolutions,tt0242653,2003,Movie;Sequel");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ACTOR = String.format(
            "id:ID,personId,name,age,:LABEL%n" +
                    "6,keanu,Keanu Reeves,45.1,Actor%n" +
                    "7,laurence,Laurence Fishburne,50,Actor%n" +
                    "8,carrieanne,Carrie-Anne Moss,40.9,Actor");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP = String.format(
            "role,:START_ID,:END_ID,:TYPE%n" +
                    "Neo,6,3,ACTED_IN%n" +
                    "Neo,6,4,ACTED_IN%n" +
                    "Neo,6,5,ACTED_IN%n" +
                    "Morpheus,7,3,ACTED_IN%n" +
                    "Morpheus,7,4,ACTED_IN%n" +
                    "Morpheus,7,5,ACTED_IN%n" +
                    "Trinity,8,3,ACTED_IN%n" +
                    "Trinity,8,4,ACTED_IN%n" +
                    "Trinity,8,5,ACTED_IN");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS = String.format(
            "id:ID;name;street;:LABEL"
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS1 = String.format(
            "id:ID;street;name;city;:LABEL"
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER = String.format(
            "id:ID;name;age:long;:LABEL"
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER1 = String.format(
            "id:ID;name;age:long;male:boolean;kids;:LABEL"
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_KNOWS = String.format(
            ":START_ID;:END_ID;:TYPE"
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_NEXT_DELIVERY = String.format(
            ":START_ID;:END_ID;:TYPE"
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS = String.format(
            "21;Bar Sport;;Address%n" +
            "22;;via Benni;Address"
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS1 = String.format(
            "20;Via Garibaldi, 7;Andrea;Milano;\"Address1;Address\""
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER = String.format(
            "1;bar;42;User%n" +
            "2;;12;User"
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER1 = String.format(
            "0;foo;42;true;[a,b,c];\"User1;User\""
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_KNOWS = String.format(
            "0;1;KNOWS"
    );

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_NEXT_DELIVERY = String.format(
            "20;21;NEXT_DELIVERY"
    );

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");
    private static MiniDFSCluster miniDFSCluster;

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);
        cleanAndCreate();
        miniDFSCluster = HdfsTestUtils.getLocalHDFSCluster();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
        miniDFSCluster.shutdown();
    }

    @Test
    public void testExportInvalidQuoteValue() throws Exception {
        try {
            File output = new File(directory, "all.csv");
            TestUtil.testCall(db, "CALL apoc.export.csv.all({file},{quote: 'Invalid'}, null)", map("file", output.getAbsolutePath()),
                    (r) -> assertResults(output, r, "database"));
            fail();
        } catch (RuntimeException e) {
            assertTrue(true);
        }
    }

    @Test
    public void testExportAllCsv() throws Exception {
        File output = new File(directory, "all.csv");
        TestUtil.testCall(db, "CALL apoc.export.csv.all({file},null)", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportAllCsvWithQuotes() throws Exception {
        File output = new File(directory, "all.csv");
        TestUtil.testCall(db, "CALL apoc.export.csv.all({file},{quotes: true})", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportAllCsvWithoutQuotes() throws Exception {
        File output = new File(directory, "all.csv");
        TestUtil.testCall(db, "CALL apoc.export.csv.all({file},{quotes: 'none'})", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_NONE_QUOTES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportAllCsvNeededQuotes() throws Exception {
        File output = new File(directory, "all.csv");
        TestUtil.testCall(db, "CALL apoc.export.csv.all({file},{quotes: 'ifNeeded'})", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_NEEDED_QUOTES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportAllCsvHDFS() throws Exception {
        String hdfsUrl = String.format("hdfs://localhost:12345/user/%s/all.csv", System.getProperty("user.name"));
        TestUtil.testCall(db, "CALL apoc.export.csv.all({file},null)", map("file", hdfsUrl),
                (r) -> {
                    try {
                        FileSystem fs = miniDFSCluster.getFileSystem();
                        FSDataInputStream inputStream = fs.open(new Path(String.format("/user/%s/all.csv", System.getProperty("user.name"))));
                        File output = Files.createTempFile("all", ".csv").toFile();
                        FileUtils.copyInputStreamToFile(inputStream, output);
                        assertEquals(6L, r.get("nodes"));
                        assertEquals(2L, r.get("relationships"));
                        assertEquals(12L, r.get("properties"));
                        assertEquals("database: nodes(6), rels(2)", r.get("source"));
                        assertEquals("csv", r.get("format"));
                        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }

    @Test
    public void testExportGraphCsv() throws Exception {
        File output = new File(directory, "graph.csv");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.csv.graph(graph, {file},{quotes: 'none'}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_NONE_QUOTES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphCsvWithoutQuotes() throws Exception {
        File output = new File(directory, "graph.csv");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.csv.graph(graph, {file},null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryCsv() throws Exception {
        File output = new File(directory, "query.csv");
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.csv.query({query},{file},null)", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryCsvWithoutQuotes() throws Exception {
        File output = new File(directory, "query.csv");
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.csv.query({query},{file},{quotes: false})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_WITHOUT_QUOTES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryNodesCsv() throws Exception {
        File output = new File(directory, "query_nodes.csv");
        String query = "MATCH (u:User) return u";
        TestUtil.testCall(db, "CALL apoc.export.csv.query({query},{file},null)", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_NODES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportQueryNodesCsvParams() throws Exception {
        File output = new File(directory, "query_nodes.csv");
        String query = "MATCH (u:User) WHERE u.age > {age} return u";
        TestUtil.testCall(db, "CALL apoc.export.csv.query({query},{file},{params:{age:10}})", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_NODES, new Scanner(output).useDelimiter("\\Z").next());
    }

    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertEquals(6L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(12L, r.get("properties"));
        assertEquals(source + ": nodes(6), rels(2)", r.get("source"));
        assertEquals(output.getAbsolutePath(), r.get("file"));
        assertEquals("csv", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    @Test public void testExportAllCsvStreaming() throws Exception {
        String statement = "CALL apoc.export.csv.all(null,{stream:true,batchSize:2})";
        StringBuilder sb=new StringBuilder();
        TestUtil.testResult(db, statement, (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(2L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(6L, r.get("properties"));
            assertNull("Should get file",r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(4L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(10L, r.get("properties"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(3L, r.get("batches"));
            assertEquals(6L, r.get("nodes"));
            assertEquals(6L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(4L, r.get("batches"));
            assertEquals(6L, r.get("nodes"));
            assertEquals(8L, r.get("rows"));
            assertEquals(2L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
        });
        assertEquals(EXPECTED+"\n",sb.toString());
    }

    @Test public void testCypherCsvStreaming() throws Exception {
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.csv.query({query},null,{stream:true,batchSize:2})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchUsers(sb));
        assertEquals(EXPECTED_QUERY+"\n",sb.toString());
    }

    @Test public void testCypherCsvStreamingWithoutQuotes() throws Exception {
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.csv.query({query},null,{quotes: false, stream:true,batchSize:2})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchUsers(sb));

        assertEquals(EXPECTED_QUERY_WITHOUT_QUOTES+"\n",sb.toString());
    }

    private Consumer<Result> getAndCheckStreamingMetadataQueryMatchUsers(StringBuilder sb)
    {
        return (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(10L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0",
                    ((long) r.get("time")) >= 0);
            sb.append(r.get("data")); r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(15L, r.get("properties"));
            assertTrue("Should get time greater than 0",
                    ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
        };
    }

    @Test public void testCypherCsvStreamingWithAlwaysQuotes() throws Exception {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.csv.query({query},null,{quotes: 'always', stream:true,batchSize:2})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_ALWAYS+"\n",sb.toString());
    }

    @Test public void testCypherCsvStreamingWithNeededQuotes() throws Exception {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.csv.query({query},null,{quotes: 'ifNeeded', stream:true,batchSize:2})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_NEEDED + "\n",sb.toString());
    }

    @Test public void testCypherCsvStreamingWithNoneQuotes() throws Exception {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.csv.query({query},null,{quotes: 'none', stream:true,batchSize:2})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_NONE + "\n",sb.toString());
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImportWithConfig() throws Exception {

        File dir = new File(directory, "query_nodes.csv");

        TestUtil.testCall(db, "CALL apoc.export.csv.all({directory},{bulkImport: true, separateHeader: true, delim: ';'})",
                map("directory", dir.getAbsolutePath()), r -> {
                    assertEquals(20000L, r.get("batchSize"));
                    assertEquals(1L, r.get("batches"));
                    assertEquals(6L, r.get("nodes"));
                    assertEquals(8L, r.get("rows"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(12L, r.get("properties"));
                    assertTrue("Should get time greater than 0",
                            ((long) r.get("time")) >= 0);
                }
        );

        String file = dir.getParent() + File.separator;
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS, "query_nodes.header.nodes.Address.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS1, "query_nodes.header.nodes.Address1.Address.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER, "query_nodes.header.nodes.User.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER1, "query_nodes.header.nodes.User1.User.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_KNOWS, "query_nodes.header.relationships.KNOWS.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_NEXT_DELIVERY, "query_nodes.header.relationships.NEXT_DELIVERY.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS, "query_nodes.nodes.Address.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS1, "query_nodes.nodes.Address1.Address.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER, "query_nodes.nodes.User.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER1, "query_nodes.nodes.User1.User.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_KNOWS, "query_nodes.relationships.KNOWS.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_NEXT_DELIVERY, "query_nodes.relationships.NEXT_DELIVERY.csv");
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImport() throws Exception {

        File dir = new File(directory, "query_nodes.csv");

        db.execute("MATCH (n) detach delete (n)").close();

        String movieDb = "CREATE (m1:`Movie` {`movieId`:\"tt0133093\", `title`:\"The Matrix\", `year`:1999}),\n" +
                "(m2:`Movie`:`Sequel` {`movieId`:\"tt0234215\", `title`:\"The Matrix Reloaded\", `year`:2003}),\n" +
                "(m3:`Movie`:`Sequel` {`movieId`:\"tt0242653\", `title`:\"The Matrix Revolutions\", `year`:2003}),\n" +
                "(a1:`Actor` {`name`:\"Keanu Reeves\", `personId`:\"keanu\", `age`: 45.1}),\n" +
                "(a2:`Actor` {`name`:\"Laurence Fishburne\", `personId`:\"laurence\", `age`: 50}),\n" +
                "(a3:`Actor` {`name`:\"Carrie-Anne Moss\", `personId`:\"carrieanne\", `age`: 40.9}),\n" +
                "(a1)-[r1:`ACTED_IN` {`role`:\"Neo\"}]->(m1),\n" +
                "(a1)-[r2:`ACTED_IN` {`role`:\"Neo\"}]->(m2), \n" +
                "(a1)-[r3:`ACTED_IN` {`role`:\"Neo\"}]->(m3), \n" +
                "(a2)-[r4:`ACTED_IN` {`role`:\"Morpheus\"}]->(m1),\n" +
                "(a2)-[r5:`ACTED_IN` {`role`:\"Morpheus\"}]->(m2), \n" +
                "(a2)-[r6:`ACTED_IN` {`role`:\"Morpheus\"}]->(m3), \n" +
                "(a3)-[r7:`ACTED_IN` {`role`:\"Trinity\"}]->(m1),\n" +
                "(a3)-[r8:`ACTED_IN` {`role`:\"Trinity\"}]->(m2), \n" +
                "(a3)-[r9:`ACTED_IN` {`role`:\"Trinity\"}]->(m3), \n" +
                "(p:Product {categoryID: 1, discontinued: false, productID: 1, productName: 'Chai', quantityPerUnit: '10 boxes x 20 bags', reorderLevel: 10, supplierID: 1, unitPrice: 18.0, unitsInStock: 39, unitsOnOrder: 0}), \n" +
                "(a:Test {date: date('2018-10-30'), localDateTime: localdatetime('20181030T19:32:24'), dateTime: datetime('2018-10-30T12:50:35.556+0100'), localtime: localtime('12:50:35.556'), duration: duration('P5M1DT12H'), time: time('125035.556+0100'), born_2D: point({ x: 2.3, y: 4.5 }), born_3D:point({ longitude: 56.7, latitude: 12.78, height: 100 })})";
        db.execute(movieDb).close();
        TestUtil.testCall(db, "CALL apoc.export.csv.all({directory},{bulkImport: true})",
                map("directory", dir.getAbsolutePath()), r -> {
                    assertEquals(20000L, r.get("batchSize"));
                    assertEquals(1L, r.get("batches"));
                    assertEquals(8L, r.get("nodes"));
                    assertEquals(17L, r.get("rows"));
                    assertEquals(9L, r.get("relationships"));
                    assertEquals(45L, r.get("properties"));
                    assertTrue("Should get time greater than 0",
                            ((long) r.get("time")) >= 0);
                }
        );
        String file = dir.getParent() + File.separator;
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_NODE_PRODUCT, "query_nodes.nodes.Product.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_NODE_MOVIE, "query_nodes.nodes.Movie.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ACTOR, "query_nodes.nodes.Actor.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_NODE_MOVIE_SEQUEL, "query_nodes.nodes.Movie.Sequel.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP, "query_nodes.relationships.ACTED_IN.csv");
        assertFileEquals(file, EXPECTED_NEO4J_ADMIN_IMPORT_TEST_NODE, "query_nodes.nodes.Test.csv");
        cleanAndCreate();
    }

    @Test
    public void testExportGraphNeo4jAdminCsv() throws Exception {
        File output = new File(directory, "graph.csv");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.csv.graph(graph, {file},{bulkImport: true, delim: ';'}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));

        String file = output.getParent() + File.separator;
        assertFileEquals(file,EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS +"\n"+ EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS, "graph.nodes.Address.csv");
        assertFileEquals(file,EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS1 +"\n"+ EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS1, "graph.nodes.Address1.Address.csv");
        assertFileEquals(file,EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER +"\n"+ EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER, "graph.nodes.User.csv");
        assertFileEquals(file,EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER1 +"\n"+ EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER1, "graph.nodes.User1.User.csv");
        assertFileEquals(file,EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_KNOWS +"\n"+ EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_KNOWS, "graph.relationships.KNOWS.csv");
        assertFileEquals(file,EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_NEXT_DELIVERY +"\n"+ EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_NEXT_DELIVERY, "graph.relationships.NEXT_DELIVERY.csv");
    }

    private static void cleanAndCreate() {
        db.execute("MATCH (n) detach delete (n)").close();
        db.execute("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})").close();
        db.execute("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})").close();
    }

    private void assertFileEquals(String file, String expectedNeo4jAdminImportNodeProduct, String s) throws FileNotFoundException {
        assertEquals(expectedNeo4jAdminImportNodeProduct, new Scanner(new File(file + s)).useDelimiter("\\Z").next());
    }

    @Test(expected = RuntimeException.class)
    public void testCypherExportCsvForAdminNeo4jImportException() throws Exception {
        File dir = new File(directory, "query_nodes.csv");
        try {
            TestUtil.testCall(db, "CALL apoc.export.csv.query('MATCH (n) return (n)',{directory},{bulkImport: true})", Util.map("directory", dir.getAbsolutePath()), (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("You can use the `bulkImport` only with apoc.export.all and apoc.export.csv.graph", except.getMessage());
            throw e;
        }
        try {
            TestUtil.testCall(db, "CALL apoc.export.csv.data('MATCH (n) return (n)',{directory},{bulkImport: true})", Util.map("directory", dir.getAbsolutePath()), (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("Only apoc.export.all can have the `neo4jAdmin` config", except.getMessage());
            throw e;
        }
        try {
            TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph CALL apoc.export.csv.graph(graph,{directory},{bulkImport: true})", Util.map("directory", dir.getAbsolutePath()), (r) -> {});
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("Only apoc.export.all can have the `neo4jAdmin` config", except.getMessage());
            throw e;
        }
    }

    private Consumer<Result> getAndCheckStreamingMetadataQueryMatchAddress(StringBuilder sb)
    {
        return (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(8L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0",
                    ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0",
                    ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
        };
    }
}