package apoc.export.csv;

import apoc.export.xls.ExportXls;
import apoc.graph.Graphs;
import apoc.load.LoadXls;
import apoc.util.CompressionAlgo;
import apoc.util.CompressionConfig;
import apoc.util.TestUtil;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
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
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExportXls.class, LoadXls.class, Graphs.class);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        db.executeTransactionally("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c'],location:point({longitude: 11.8064153, latitude: 48.1716114}),dob:date({ year:1984, month:10, day:11 }), created: datetime()})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
        db.executeTransactionally("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
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
    public void testExportAllXlsWithCompression() {
        final CompressionAlgo algo = CompressionAlgo.GZIP;
        String fileName = "all.xlsx.gz";
        TestUtil.testCall(db, "CALL apoc.export.xls.all($file, $config)",
                map("file", fileName, "config", map("compression", algo.name())),
                (r) -> assertResults(fileName, r, "database"));

        assertExcelFileForGraph(fileName, algo);

        // check xls through load.xls
        TestUtil.testResult(db, "CALL apoc.load.xls($file, 'Address', $config)",
                map("file", fileName, "config", map(CompressionConfig.COMPRESSION, algo.name())),
                (r) -> {
                    final Set<Object> actual = Iterators.stream(r.<Map>columnAs("map"))
                            .map(i -> Optional.ofNullable(i.get("name")).orElse(""))
                            .collect(Collectors.toSet());
                    assertEquals(3,actual.size());
                    assertEquals(Set.of("Andrea", "Bar Sport", ""), actual);
                });
    }

    @Test
    public void testExportGraphXls()  {
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
    public void testExportGraphXlsWithMoreThan100Nodes()  {
        db.executeTransactionally("UNWIND range(1,200) as range CREATE (n:Test)");
        String fileName = "graph.xlsx";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.xls.graph(graph, $file,null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "graph", 208L, 2L, 206));
        assertExcelFileForGraph(fileName);
        db.executeTransactionally("MATCH (n:Test) DETACH DELETE n");
    }

    @Test
    public void testExportGraphXlsWithCustomHeaderAndMoreThan100Nodes() {
        String nodeId = "customNode";
        String relId = "customRel";
        String startNodeId = "customStart";
        String endNodeId = "customEnd";
        db.executeTransactionally("UNWIND range(1,200) as range CREATE (n:Test)");
        String fileName = "graph.xlsx";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.xls.graph(graph, $file, $conf) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("file", fileName, "conf", map("headerNodeId", nodeId,
                        "headerRelationshipId", relId, "headerStartNodeId", startNodeId, "headerEndNodeId", endNodeId)),
                (r) -> assertResults(fileName, r, "graph", 208L, 2L, 206));
        assertExcelFileForGraph(fileName, nodeId, List.of(relId, startNodeId, endNodeId), CompressionAlgo.NONE);
        db.executeTransactionally("MATCH (n:Test) DETACH DELETE n");
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


    private void assertResults(String fileName, Map<String, Object> r, final String source, long expectedNodes, long expectedRels, int expectedNodesSource) {
        assertEquals(expectedNodes, r.get("nodes")); // we're exporting nodes with multiple label multiple times
        assertEquals(2L, r.get("relationships"));
        assertEquals(25L, r.get("properties"));
        assertEquals(source + String.format(": nodes(%s), rels(%s)", expectedNodesSource, expectedRels), r.get("source"));
        assertEquals(fileName, r.get("file"));
        assertEquals("xls", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertResults(fileName, r, source, 8L, 2L, 6);
    }

    private void assertExcelFileForGraph(String fileName) {
        assertExcelFileForGraph(fileName, CompressionAlgo.NONE);
    }

    private void assertExcelFileForGraph(String fileName, CompressionAlgo algo) {
        assertExcelFileForGraph(fileName, "<nodeId>", List.of("<relationshipId>", "<startNodeId>", "<endNodeId>"), algo);
    }

    private void assertExcelFileForGraph(String fileName, String headerNode, List<String> headerRel, CompressionAlgo algo) {
        try (InputStream fileInputStream = Files.newInputStream(new File(directory, fileName).toPath());
             InputStream inp = algo.getInputStream(fileInputStream);
             Transaction tx = db.beginTx()) {
            Workbook wb = WorkbookFactory.create(inp);

            int numberOfSheets = wb.getNumberOfSheets();
            assertEquals(Iterables.count(tx.getAllLabelsInUse()) + Iterables.count(tx.getAllRelationshipTypesInUse()), numberOfSheets);

            for (Label label: tx.getAllLabelsInUse()) {
                long numberOfNodes = Iterators.count(tx.findNodes(label));
                Sheet sheet = wb.getSheet(label.name());
                assertEquals(numberOfNodes, sheet.getLastRowNum());
                final Set<String> actual = Iterators.stream(sheet.getRow(0).cellIterator()).map(Cell::getStringCellValue).collect(Collectors.toSet());
                final Set<String> expected = StreamSupport.stream(tx.getAllNodes().spliterator(), false)
                        .filter(node -> node.hasLabel(label))
                        .flatMap(i -> StreamSupport.stream(i.getPropertyKeys().spliterator(), false))
                        .collect(Collectors.toSet());
                expected.add(headerNode);
                assertEquals(expected, actual);
            }
            for (RelationshipType relType: tx.getAllRelationshipTypesInUse()) {
                long numberOfRels = tx.getAllRelationships().stream().filter(rel -> rel.isType(relType)).count();
                Sheet sheet = wb.getSheet(relType.name());
                assertEquals(numberOfRels, sheet.getLastRowNum());
                final Set<String> actual = Iterators.stream(sheet.getRow(0).cellIterator()).map(Cell::getStringCellValue).collect(Collectors.toSet());
                final Set<String> expected = StreamSupport.stream(tx.getAllRelationships().spliterator(), false)
                        .filter(rel -> rel.isType(relType))
                        .flatMap(i -> StreamSupport.stream(i.getPropertyKeys().spliterator(), false))
                        .collect(Collectors.toSet());
                expected.addAll(headerRel);
                assertEquals(expected, actual);
            }
            tx.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertSheets(String fileName, int sheets) {
        try (InputStream inp = Files.newInputStream(new File(directory, fileName).toPath())) {
            Workbook wb = WorkbookFactory.create(inp);

            int numberOfSheets = wb.getNumberOfSheets();
            assertEquals(sheets, numberOfSheets);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertExcelFileForQuery(String fileName) {
        assertSheets(fileName, 1);
    }
}
