package apoc.export.csv;

import apoc.export.xls.ExportXls;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExportXlsTest {

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

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

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
        TestUtil.registerProcedure(db, ExportXls.class, Graphs.class);
        db.execute("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c'],location:point({longitude: 11.8064153, latitude: 48.1716114}),dob:date({ year:1984, month:10, day:11 }), created: datetime()})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})").close();
        db.execute("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})").close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExportAllXls() throws Exception {
        String fileName = "all.xlsx";
        TestUtil.testCall(db, "CALL apoc.export.xls.all({file},null)",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));

        assertExcelFileForGraph(fileName);
    }

    @Test
    public void testExportGraphXls() throws Exception {
        String fileName = "graph.xlsx";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.xls.graph(graph, {file},null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "graph"));
        assertExcelFileForGraph(fileName);
    }

    @Test
    public void testExportQueryXls() throws Exception {
        String fileName = "query.xlsx";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, "CALL apoc.export.xls.query({query},{file},null)",
                map("file", fileName, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("xls", r.get("format"));

                });
        assertExcelFileForQuery(fileName);
    }


    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertEquals(8L, r.get("nodes")); // we're exporting nodes with multiple label multiple times
        assertEquals(2L, r.get("relationships"));
        assertEquals(25L, r.get("properties"));
        assertEquals(source + ": nodes(6), rels(2)", r.get("source"));
        assertEquals(fileName, r.get("file"));
        assertEquals("xls", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    private void assertExcelFileForGraph(String fileName) {
        try (InputStream inp = new FileInputStream(new File(directory, fileName)); Transaction tx = db.beginTx()) {
            Workbook wb = WorkbookFactory.create(inp);

            int numberOfSheets = wb.getNumberOfSheets();
            assertEquals(Iterables.count(db.getAllLabels()) + Iterables.count(db.getAllRelationshipTypes()), numberOfSheets);

            for (Label label: db.getAllLabels()) {
                long numberOfNodes = Iterators.count(db.findNodes(label));
                Sheet sheet = wb.getSheet(label.name());
                assertEquals(numberOfNodes, sheet.getLastRowNum());
            }
            tx.success();
        } catch (IOException|InvalidFormatException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertExcelFileForQuery(String fileName) {
        try (InputStream inp = new FileInputStream(new File(directory, fileName))) {
            Workbook wb = WorkbookFactory.create(inp);

            int numberOfSheets = wb.getNumberOfSheets();
            assertEquals(1, numberOfSheets);

        } catch (IOException|InvalidFormatException e) {
            throw new RuntimeException(e);
        }
    }
}
