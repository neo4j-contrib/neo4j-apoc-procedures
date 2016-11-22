package apoc.export.csv;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.Map;
import java.util.Scanner;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCsvTest {

    private static final String EXPECTED_QUERY_NODES = String.format("\"u\"%n" +
            "\"{\"\"id\"\":0,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"name\"\":\"\"foo\"\",\"\"age\"\":42,\"\"male\"\":true,\"\"kids\"\":[\"\"a\"\",\"\"b\"\",\"\"c\"\"]}}\"%n" +
            "\"{\"\"id\"\":1,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"name\"\":\"\"bar\"\",\"\"age\"\":42}}\"%n" +
            "\"{\"\"id\"\":2,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"age\"\":12}}\"");
    private static final String EXPECTED_QUERY = String.format("\"u.age\",\"u.name\",\"u.male\",\"u.kids\",\"labels(u)\"%n" +
            "\"42\",\"foo\",\"true\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"[\"\"User\"\"]\"%n" +
            "\"42\",\"bar\",\"\",\"\",\"[\"\"User\"\"]\"%n" +
            "\"12\",\"\",\"\",\"\",\"[\"\"User\"\"]\"");
    private static final String EXPECTED = String.format("\"_id\",\"_labels\",\"name\",\"age\",\"male\",\"kids\",\"_start\",\"_end\",\"_type\"%n" +
            "\"0\",\":User\",\"foo\",\"42\",\"true\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",,,%n" +
            "\"1\",\":User\",\"bar\",\"42\",\"\",\"\",,,%n" +
            "\"2\",\":User\",\"\",\"12\",\"\",\"\",,,%n" +
            ",,,,,,\"0\",\"1\",\"KNOWS\"");

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
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);
        db.execute("CREATE (f:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})").close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExportAllCsv() throws Exception {
        File output = new File(directory, "all.csv");
        TestUtil.testCall(db, "CALL apoc.export.csv.all({file},null)", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphCsv() throws Exception {
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
                    assertEquals(true,r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY, new Scanner(output).useDelimiter("\\Z").next());
    }
    @Test
    public void testExportQueryNodesCsv() throws Exception {
        File output = new File(directory, "query_nodes.csv");
        String query = "MATCH (u:User) return u";
        TestUtil.testCall(db, "CALL apoc.export.csv.query({query},{file},null)", map("file", output.getAbsolutePath(),"query",query),
                (r) -> {
                    assertEquals(true,r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_NODES, new Scanner(output).useDelimiter("\\Z").next());
    }

    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(7L, r.get("properties"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals(output.getAbsolutePath(), r.get("file"));
        assertEquals("csv", r.get("format"));
        assertEquals(true, ((long) r.get("time")) > 0);
    }
}
