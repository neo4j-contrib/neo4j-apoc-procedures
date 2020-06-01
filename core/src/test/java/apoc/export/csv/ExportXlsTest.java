package apoc.export.csv;

import apoc.ApocSettings;
import apoc.export.xls.ExportXls;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExportXlsTest {

    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_export_file_enabled, true);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExportXls.class, Graphs.class);
        db.executeTransactionally("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c'],location:point({longitude: 11.8064153, latitude: 48.1716114}),dob:date({ year:1984, month:10, day:11 }), created: datetime()})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
        db.executeTransactionally("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
    }

    @Test
    public void testExportAllXls() throws Exception {
        String fileName = "all.xlsx";
        TestUtil.testCall(db, "CALL apoc.export.xls.all($file,null)",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));

        assertExcelFileForGraph(fileName);
    }

    @Test
    public void testExportGraphXls() throws Exception {
        String fileName = "graph.xlsx";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.xls.graph(graph, $file,null) " +
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
        TestUtil.testCall(db, "CALL apoc.export.xls.query($query,$file,null)",
                map("file", fileName, "query", query),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("xls", r.get("format"));

                });
        assertExcelFileForQuery(fileName);
    }

    @Test
    public void testExportQueryXlsWithSameLabelAndRelName() {
        db.executeTransactionally("CREATE (u:User{name: 'Andrea'})-[r:COMPANY{since: 2018}]->(c:Company{name: 'Larus'})");
        String fileName = "query.xlsx";
        TestUtil.testCall(db, "CALL apoc.graph.fromCypher($query, {}, '', {}) YIELD graph AS exportedGraph " +
                        "CALL apoc.export.xls.graph(exportedGraph, $file, $exportConf) YIELD file, source, format, time " +
                        "RETURN *",
                map("file", fileName,
                        "exportConf", map("prefixSheetWithEntityType", true),
                        "query", "MATCH (u:User{name: 'Andrea'})-[r:COMPANY]->(c:Company{name: 'Larus'}) return *"),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().equals("graph: nodes(2), rels(1)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("xls", r.get("format"));
                });
        assertSheets(fileName, 3);
        db.executeTransactionally("MATCH p = (u:User{name: 'Andrea'})-[r:COMPANY]->(c:Company{name: 'Larus'}) DELETE p");
    }

    @Test
    public void testExportQueryXlsWithJoinedLabels() {
        db.executeTransactionally("CREATE (u:User:Customer{name: 'Andrea'})-[r:COMPANY{since: 2018}]->(c:Company:Customer{name: 'Larus'})");
        String fileName = "query.xlsx";
        TestUtil.testCall(db, "CALL apoc.graph.fromCypher($query, {}, '', {}) YIELD graph AS exportedGraph " +
                        "CALL apoc.export.xls.graph(exportedGraph, $file, $exportConf) YIELD file, source, format, time " +
                        "RETURN *",
                map("file", fileName,
                        "exportConf", map("prefixSheetWithEntityType", true, "joinLabels", true),
                        "query", "MATCH (u:User{name: 'Andrea'})-[r:COMPANY]->(c:Company{name: 'Larus'}) return *"),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().equals("graph: nodes(2), rels(1)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("xls", r.get("format"));
                });
        assertSheets(fileName, 3);
        db.executeTransactionally("MATCH p = (u:User{name: 'Andrea'})-[r:COMPANY]->(c:Company{name: 'Larus'}) DELETE p");
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
            assertEquals(Iterables.count(tx.getAllLabelsInUse()) + Iterables.count(tx.getAllRelationshipTypesInUse()), numberOfSheets);

            for (Label label: tx.getAllLabelsInUse()) {
                long numberOfNodes = Iterators.count(tx.findNodes(label));
                Sheet sheet = wb.getSheet(label.name());
                assertEquals(numberOfNodes, sheet.getLastRowNum());
            }
            tx.commit();
        } catch (IOException|InvalidFormatException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertSheets(String fileName, int sheets) {
        try (InputStream inp = new FileInputStream(new File(directory, fileName))) {
            Workbook wb = WorkbookFactory.create(inp);

            int numberOfSheets = wb.getNumberOfSheets();
            assertEquals(sheets, numberOfSheets);

        } catch (IOException|InvalidFormatException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertExcelFileForQuery(String fileName) {
        assertSheets(fileName, 1);
    }
}
