package apoc.export.cypher;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.*;
import static apoc.export.util.ExportFormat.*;
import static apoc.util.Util.map;
import static org.junit.Assert.*;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypherTest {

    private static final Map<String, Object> exportConfig = map("useOptimizations", map("type", "none"), "separateFiles", true, "format", "neo4j-admin");

    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(newBuilder( "unsupported.dbms.debug.track_cursor_close", BOOL, false ).build(), false)
            .withSetting(newBuilder( "unsupported.dbms.debug.trace_cursors", BOOL, false ).build(), false);

    @Rule
    public TestName testName = new TestName();

    private static final String OPTIMIZED = "Optimized";
    private static final String ODD = "OddDataset";

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, ExportCypher.class, Graphs.class);
        db.executeTransactionally("CREATE INDEX ON :Bar(first_name, last_name)");
        db.executeTransactionally("CREATE INDEX ON :Foo(name)");
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE");
        if (testName.getMethodName().endsWith(OPTIMIZED)) {
            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar:Person {age:12}),(d:Bar {age:12})," +
                    " (t:Foo {name:'foo2', born:date('2017-09-29')})-[:KNOWS {since:2015}]->(e:Bar {name:'bar2',age:44}),({age:99})");
        } else if(testName.getMethodName().endsWith(ODD)) {
            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})," +
                    "(t:Foo {name:'foo2', born:date('2017-09-29')})," +
                    "(g:Foo {name:'foo3', born:date('2016-03-12')})," +
                    "(b:Bar {name:'bar',age:42})," +
                    "(c:Bar {age:12})," +
                    "(d:Bar {age:4})," +
                    "(e:Bar {name:'bar2',age:44})," +
                    "(f)-[:KNOWS {since:2016}]->(b)");
        } else {
            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})");
        }
    }

    @Test
    public void testExportAllCypherResults() {
        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null,{useOptimizations: { type: 'none'}, format: 'neo4j-shell'})", (r) -> {
            assertResults(null, r, "database");
            assertEquals(EXPECTED_NEO4J_SHELL, r.get("cypherStatements"));
        });
    }

    @Test
    public void testExportAllCypherStreaming() {
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.cypher.all(null,{useOptimizations: { type: 'none'}, streamStatements:true,batchSize:3, format: 'neo4j-shell'})", (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(5L, r.get("properties"));
            assertNull("Should get file",r.get("file"));
            assertEquals("cypher", r.get("format"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(r.get("cypherStatements"));
            r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(1L, r.get("relationships"));
            assertEquals(6L, r.get("properties"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(r.get("cypherStatements"));
        });
        assertEquals(EXPECTED_NEO4J_SHELL.replace("LIMIT 20000", "LIMIT 3"), sb.toString());
    }

    // -- Whole file test -- //
    @Test
    public void testExportAllCypherDefault() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($fileName,{useOptimizations: { type: 'none'}, format: 'neo4j-shell'})",
                map("fileName", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(fileName));
    }

    @Test
    public void testExportAllCypherForCypherShell() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$config)",
                map("file", fileName, "config", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_CYPHER_SHELL, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherForNeo4j() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(fileName));
    }

    private static String readFile(String fileName) throws FileNotFoundException {
        return TestUtil.readFileToString(new File(directory, fileName));
    }

    @Test
    public void testExportGraphCypher() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", fileName, "exportConfig", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(fileName));
    }

    // -- Separate files tests -- //
    @Test
    public void testExportAllCypherNodes() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NODES, readFile("all.nodes.cypher"));
    }

    @Test
    public void testExportAllCypherRelationships() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile("all.relationships.cypher"));
    }

    @Test
    public void testExportAllCypherSchema() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_SCHEMA, readFile("all.schema.cypher"));
    }

    @Test
    public void testExportAllCypherCleanUp() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("all.cleanup.cypher"));
    }

    @Test
    public void testExportGraphCypherNodes() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", fileName, "exportConfig", exportConfig), (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NODES, readFile("graph.nodes.cypher"));
    }

    @Test
    public void testExportGraphCypherRelationships() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile("graph.relationships.cypher"));
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_SCHEMA, readFile("graph.schema.cypher"));
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("graph.cleanup.cypher"));
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(6L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportQueryCypherPlainFormat() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "plain")), (r) -> {
                });
        assertEquals(EXPECTED_PLAIN, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatUpdateAll() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateAll")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_MERGE, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "addStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_MERGE_ON_CREATE_SET + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP_EMPTY, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_EMPTY + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET + EXPECTED_CLEAN_UP_EMPTY, readFile(fileName));
    }

    @Test
    public void testExportSchemaCypher() throws Exception {
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig), (r) -> {
        });
        assertEquals(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL, readFile(fileName));
    }

    @Test
    public void testExportSchemaCypherShell() throws Exception {
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($file,$exportConfig)",
                map("file", fileName, "exportConfig", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, readFile(fileName));
    }

    @Test
    public void testExportCypherNodePoint() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:Test {name:'foo'," +
                "place2d:point({ x: 2.3, y: 4.5 })," +
                "place3d1:point({ x: 2.3, y: 4.5 , z: 1.2})})" +
                "-[:FRIEND_OF {place2d:point({ longitude: 56.7, latitude: 12.78 })}]->" +
                "(:Bar {place3d:point({ longitude: 12.78, latitude: 56.7, height: 100 })})");
        String fileName = "temporalPoint.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_POINT, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeDate() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "date:date('2018-10-30'), " +
                "datetime:datetime('2018-10-30T12:50:35.556+0100'), " +
                "localTime:localdatetime('20181030T19:32:24')})" +
                "-[:FRIEND_OF {date:date('2018-10-30')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556')})");
        String fileName = "temporalDate.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_DATE, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeTime() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "local:localtime('12:50:35.556')," +
                "t:time('125035.556+0100')})" +
                "-[:FRIEND_OF {t:time('125035.556+0100')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556+0100')})");
        String fileName = "temporalTime.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_TIME, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeDuration() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "duration:duration('P5M1.5D')})" +
                "-[:FRIEND_OF {duration:duration('P5M1.5D')}]->" +
                "(:Bar {duration:duration('P5M1.5D')})");
        String fileName = "temporalDuration.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_DURATION, readFile(fileName));
    }

    @Test
    public void testExportWithAscendingLabels() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:User:User1:User0:User12 {name:'Alan'})");
        String fileName = "ascendingLabels.cypher";
        String query = "MATCH (f:User) WHERE f.name='Alan' RETURN f";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$file,$config)",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_LABELS_ASCENDEND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, format: 'neo4j-shell'})", map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file, $exportConfig)", map("file", fileName, "exportConfig", map("format", "neo4j-shell")),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NEO4J_OPTIMIZED, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultSeparatedFilesOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file, $exportConfig)",
                map("file", fileName, "exportConfig", map("separateFiles", true, "format", "neo4j-shell")),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NODES_OPTIMIZED, readFile("allDefaultOptimized.nodes.cypher"));
        assertEquals(EXPECTED_RELATIONSHIPS_OPTIMIZED, readFile("allDefaultOptimized.relationships.cypher"));
        assertEquals(EXPECTED_SCHEMA_OPTIMIZED, readFile("allDefaultOptimized.schema.cypher"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("allDefaultOptimized.cleanup.cypher"));
    }

    @Test
    public void testExportAllCypherCypherShellWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherCypherShellOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell'})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'plain', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainAddStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainAddStructureOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'plain', cypherFormat: 'addStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName), (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainUpdateStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateStructureOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'plain', cypherFormat: 'updateStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName), (r) -> {
                    assertEquals(0L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(2L, r.get("properties"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                });
        assertEquals(EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainUpdateAllWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateAllOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'plain', cypherFormat: 'updateAll', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName), (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_UPDATE_ALL_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName), (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:2})",
                map("file", fileName),
                (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD, readFile(fileName));
    }

    @Test
    @Ignore("non-deterministic index order")
    public void testExportAllCypherPlainOptimized() throws Exception {
        String fileName = "queryPlainOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query('MATCH (f:Foo)-[r:KNOWS]->(b:Bar) return f,r,b', $file,{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("file", fileName),
                (r) -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(10L, r.get("properties"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("statement: nodes(4), rels(2)", r.get("source"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                });
        String actual = readFile(fileName);
        assertTrue("expected generated output",EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED.equals(actual) || EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED2.equals(actual));
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddBatchSizeOddDataset() throws Exception {
        db.executeTransactionally("CREATE (:Bar {name:'bar3',age:35}), (:Bar {name:'bar4',age:36})");
        String fileName = "allPlainOddNew.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:3})",
                map("file", fileName),
                (r) -> {});
        db.executeTransactionally("MATCH (n:Bar {name:'bar3',age:35}), (n1:Bar {name:'bar4',age:36}) DELETE n, n1");
        String expectedNodes = String.format(":begin%n" +
                ":param rows => [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                ":param rows => [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{_id:1, properties:{born:date('2017-09-29'), name:\"foo2\"}}, {_id:2, properties:{born:date('2016-03-12'), name:\"foo3\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":param rows => [{name:\"bar\", properties:{age:42}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{name:\"bar2\", properties:{age:44}}, {name:\"bar3\", properties:{age:35}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":param rows => [{name:\"bar4\", properties:{age:36}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n");
        int expectedDropNum = 3;
        String expectedDrop = String.format(":begin%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT %d REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                ":commit%n" +
                ":begin%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT %d REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                ":commit%n" +
                ":begin%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                ":commit%n", expectedDropNum, expectedDropNum);
        String expected = (EXPECTED_SCHEMA_OPTIMIZED + expectedNodes + EXPECTED_RELATIONSHIPS_PARAMS_ODD + expectedDrop)
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());
        assertEquals(expected, readFile(fileName));
    }

    @Test
    public void exportMultiTokenIndex() {
        // given
        db.executeTransactionally("CREATE (n:TempNode {value:'value'})");
        db.executeTransactionally("CREATE (n:TempNode2 {value:'value'})");
        db.executeTransactionally("CALL db.index.fulltext.createNodeIndex('MyCoolNodeFulltextIndex',['TempNode', 'TempNode2'],['value'])");

        String query = "MATCH (t:TempNode) return t";
        String file = null;
        Map<String, Object> config = map("awaitForIndexes", 3000);
        String expected = String.format(":begin%n" +
                "CALL db.index.fulltext.createNodeIndex('MyCoolNodeFulltextIndex',['TempNode','TempNode2'],['value']);%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                ":commit%n" +
                "CALL db.awaitIndexes(3000);%n" +
                ":begin%n" +
                "UNWIND [{_id:3, properties:{value:\"value\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:TempNode;%n" +
                ":commit%n" +
                ":begin%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                ":commit%n" +
                ":begin%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                ":commit%n");

        // when
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query, $file, $config)",
                map("query", query, "file", file, "config", config),
                (r) -> {
                    // then
                    assertEquals(expected, r.get("cypherStatements"));
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void shouldFailExportMultiTokenIndexForRelationship() {
        // given
        db.executeTransactionally("CREATE (n:TempNode {value:'value'})");
        db.executeTransactionally("CREATE (n:TempNode2 {value:'value'})");
        db.executeTransactionally("CALL db.index.fulltext.createNodeIndex('MyCoolNodeFulltextIndex',['TempNode', 'TempNode2'],['value'])");

        // TODO: We can't manage full-text rel indexes because of this bug: https://github.com/neo4j/neo4j/issues/12304
        db.executeTransactionally("CREATE (s:TempNode)-[:REL{rel_value: 'the rel value'}]->(e:TempNode2)");
        db.executeTransactionally("CALL db.index.fulltext.createRelationshipIndex('MyCoolRelFulltextIndex',['REL'],['rel_value'])");
        String query = "MATCH (t:TempNode) return t";
        String file = null;
        Map<String, Object> config = map("awaitForIndexes", 3000);

        try {
            // when
            TestUtil.testCall(db, "CALL apoc.export.cypher.query($query, $file, $config)",
                    map("query", query, "file", file, "config", config),
                    (r) -> {});
        } catch (Exception e) {
            String expected = "Full-text indexes on relationships are not supported, please delete them in order to complete the process";
            assertEquals(expected, ExceptionUtils.getRootCause(e).getMessage());
            throw e;
        }
    }

    @Test
    public void shouldNotCreateUniqueImportIdForUniqueConstraint() {
        db.executeTransactionally("CREATE (n:Bar:Baz{name: 'A'})");
        String query = "MATCH (n:Baz) RETURN n";
        /* The bug was:
        UNWIND [{name:"A", _id:20, properties:{}}] AS row
        CREATE (n:Bar{name: row.name, `UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Baz;
        But should be the expected variable
         */
        final String expected = "UNWIND [{name:\"A\", properties:{}}] AS row\n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties SET n:Baz";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query, $file, $config)",
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    @Test
    public void shouldManageBigNumbersCorrectly() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        db.executeTransactionally("CREATE (:Bar{name:'Foo', var1:1.416785E32}), (:Bar{name:'Bar', var1:12E4});");
        final String expected = "UNWIND [{name:\"Foo\", properties:{var1:1.416785E32}}, {name:\"Bar\", properties:{var1:120000.0}}] AS row\n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file, $config)",
                map("file", null, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }
    
    @Test
    public void shouldQuotePropertyNameStartingWithDollarCharacter() {
        db.executeTransactionally("CREATE (n:Bar:Baz{name: 'A', `$lock`: true})");
        String query = "MATCH (n:Baz) RETURN n";
        final String expected = "UNWIND [{name:\"A\", properties:{`$lock`:true}}] AS row\n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties SET n:Baz";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query, $file, $config)",
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }
  
    @Test
    public void shouldHandleTwoLabelsWithOneUniqueConstraintEach() {
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Base) ASSERT b.id IS UNIQUE");
        db.executeTransactionally("CREATE (b:Bar:Base {id:'waBfk3z', name:'barista',age:42})" +
                "-[:KNOWS]->(f:Foo {name:'foosha', born:date('2018-10-31')})");
        String query = "MATCH (b:Bar:Base)-[r:KNOWS]-(f:Foo) RETURN b, f, r";
        /* The bug was:
        UNWIND [{start: {name:"barista"}, end: {_id:4}, properties:{}}] AS row
        MATCH (start:Bar{name: row.start.name, id: row.start.id})    // <-- Notice id here but not above in UNWIND!
        MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
        CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;
        But should be the expected variable
         */
        final String expected = "UNWIND [{start: {name:\"barista\"}, end: {_id:4}, properties:{}}] AS row\n" +
                "MATCH (start:Bar{name: row.start.name})\n" +
                "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query, $file, $config)",
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .filter(s -> s.contains("barista"))
                            .skip(1)
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    @Test
    public void shouldHandleTwoLabelsWithTwoUniqueConstraintsEach() {
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Base) ASSERT b.id IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Base) ASSERT b.oid IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Bar) ASSERT b.oname IS UNIQUE");
        db.executeTransactionally("CREATE (b:Bar:Base {id:'waBfk3z',oid:42,name:'barista',oname:'bar',age:42})" +
                "-[:KNOWS]->(f:Foo {name:'foosha', born:date('2018-10-31')})");
        String query = "MATCH (b:Bar:Base)-[r:KNOWS]-(f:Foo) RETURN b, f, r";
        final String expected = "UNWIND [{start: {name:\"barista\"}, end: {_id:4}, properties:{}}] AS row\n" +
                "MATCH (start:Bar{name: row.start.name})\n" +
                "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query, $file, $config)",
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .filter(s -> s.contains("barista"))
                            .skip(1)
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    private void assertResultsOptimized(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(2)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    private void assertResultsOdd(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    static class ExportCypherResults {

        static final String EXPECTED_NODES = String.format("BEGIN%n" +
                "CREATE (:Foo:`UNIQUE IMPORT LABEL` {born:date('2018-10-31'), name:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
                "CREATE (:Bar {age:42, name:\"bar\"});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {age:12, `UNIQUE IMPORT ID`:2});%n" +
                "COMMIT%n");

        private static final String EXPECTED_NODES_MERGE = String.format("BEGIN%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) SET n.name=\"foo\", n.born=date('2018-10-31'), n:Foo;%n" +
                "MERGE (n:Bar{name:\"bar\"}) SET n.age=42;%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) SET n.age=12, n:Bar;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_MERGE_ON_CREATE_SET =
                EXPECTED_NODES_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        static final String EXPECTED_NODES_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA = String.format("BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE INDEX ON :Foo(name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        public static final String EXPECTED_INDEXES_AWAIT = String.format("CALL db.awaitIndexes(300);%n");

        private static final String EXPECTED_INDEXES_AWAIT_QUERY = String.format("CALL db.awaitIndex(300);%n");

        static final String EXPECTED_RELATIONSHIPS = String.format("BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:Bar{name:\"bar\"}) CREATE (n1)-[r:KNOWS {since:2016}]->(n2);%n" +
                "COMMIT%n");

        private static final String EXPECTED_RELATIONSHIPS_MERGE = String.format("BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:Bar{name:\"bar\"}) MERGE (n1)-[r:KNOWS]->(n2) SET r.since=2016;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET =
                EXPECTED_RELATIONSHIPS_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        static final String EXPECTED_CLEAN_UP = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CLEAN_UP_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "COMMIT%n");

        static final String EXPECTED_ONLY_SCHEMA_NEO4J_SHELL = String.format("BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE INDEX ON :Foo(name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_CYPHER_POINT = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {name:\"foo\", place2d:point({x: 2.3, y: 4.5, crs: 'cartesian'}), place3d1:point({x: 2.3, y: 4.5, z: 1.2, crs: 'cartesian-3d'}), `UNIQUE IMPORT ID`:3});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {place3d:point({x: 12.78, y: 56.7, z: 100.0, crs: 'wgs-84-3d'}), `UNIQUE IMPORT ID`:4});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {place2d:point({x: 56.7, y: 12.78, crs: 'wgs-84'})}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_DATE = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {date:date('2018-10-30'), datetime:datetime('2018-10-30T12:50:35.556+01:00'), localTime:localdatetime('2018-10-30T19:32:24'), name:\"foo\", `UNIQUE IMPORT ID`:3});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {datetime:datetime('2018-10-30T12:50:35.556Z'), `UNIQUE IMPORT ID`:4});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {date:date('2018-10-30')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_TIME = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {local:localtime('12:50:35.556'), name:\"foo\", t:time('12:50:35.556+01:00'), `UNIQUE IMPORT ID`:3});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {datetime:datetime('2018-10-30T12:50:35.556+01:00'), `UNIQUE IMPORT ID`:4});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {t:time('12:50:35.556+01:00')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_DURATION = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {duration:duration('P5M1DT12H'), name:\"foo\", `UNIQUE IMPORT ID`:3});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {duration:duration('P5M1DT12H'), `UNIQUE IMPORT ID`:4});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {duration:duration('P5M1DT12H')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_LABELS_ASCENDEND = String.format("BEGIN%n" +
                "CREATE (:User:User0:User1:User12:`UNIQUE IMPORT LABEL` {name:\"Alan\", `UNIQUE IMPORT ID`:3});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA_OPTIMIZED = String.format("BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE INDEX ON :Foo(name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE = String.format("BEGIN%n" +
                "UNWIND [{_id:3, properties:{age:12}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{_id:3, properties:{age:12}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_QUERY_NODES_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_QUERY_NODES_OPTIMIZED2 = String.format("BEGIN%n" +
                "UNWIND [{_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}, {_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_ODD = String.format("BEGIN%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_PARAMS_ODD = String.format(
                "BEGIN%n" +
                ":param rows => [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}]%n" +
                "UNWIND $rows AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String DROP_UNIQUE_OPTIMIZED = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String DROP_UNIQUE_OPTIMIZED_BATCH = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_UNWIND = String.format("BEGIN%n" +
                "UNWIND [{_id:3, properties:{age:12}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_ODD = String.format("BEGIN%n" +
                "UNWIND [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:1, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:2, properties:{born:date('2016-03-12'), name:\"foo3\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_PARAMS_BATCH_SIZE_ODD = String.format(
                ":begin%n" +
                ":param rows => [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:1, properties:{born:date('2017-09-29'), name:\"foo2\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{_id:2, properties:{born:date('2016-03-12'), name:\"foo3\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":param rows => [{name:\"bar\", properties:{age:42}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{name:\"bar2\", properties:{age:44}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n");

        static final String EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND = String.format("UNWIND [{_id:3, properties:{age:12}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "MERGE (n:Bar{name: row.name}) ON CREATE SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties;%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end)  SET r += row.properties;%n");

        static final String EXPECTED_UPDATE_ALL_UNWIND = String.format("CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE INDEX ON :Foo(name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "UNWIND [{_id:3, properties:{age:12}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "MERGE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "MERGE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n");

        static final String EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND = String.format("UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "MERGE (start)-[r:KNOWS]->(end) SET r += row.properties;%n");

        static final String EXPECTED_NEO4J_OPTIMIZED = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_NEO4J_SHELL_OPTIMIZED = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_NEO4J_SHELL_OPTIMIZED_BATCH_SIZE = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_QUERY_NODES =  EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_QUERY_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;
        static final String EXPECTED_QUERY_NODES2 =  EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_QUERY_NODES_OPTIMIZED2 + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_UNWIND = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_UNWIND + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED_BATCH;

        static final String EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_ODD = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_ODD + EXPECTED_RELATIONSHIPS_ODD + DROP_UNIQUE_OPTIMIZED_BATCH;

        static final String EXPECTED_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_PARAMS_BATCH_SIZE_ODD + EXPECTED_RELATIONSHIPS_PARAMS_ODD + DROP_UNIQUE_OPTIMIZED_BATCH;


        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND = EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_UNWIND
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());


        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD = EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_ODD
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD = EXPECTED_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED = EXPECTED_QUERY_NODES
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT_QUERY)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED2 = EXPECTED_QUERY_NODES2
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT_QUERY)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_CYPHER_SHELL_OPTIMIZED = EXPECTED_NEO4J_SHELL_OPTIMIZED
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE = EXPECTED_NEO4J_SHELL_OPTIMIZED_BATCH_SIZE
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE = EXPECTED_NEO4J_SHELL_OPTIMIZED_BATCH_SIZE
                .replace(NEO4J_SHELL.begin(), PLAIN_FORMAT.begin())
                .replace(NEO4J_SHELL.commit(), PLAIN_FORMAT.commit())
                .replace(NEO4J_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

        static final String EXPECTED_NEO4J_SHELL = EXPECTED_NODES + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP;

        static final String EXPECTED_CYPHER_SHELL = EXPECTED_NEO4J_SHELL
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_PLAIN = EXPECTED_NEO4J_SHELL
                .replace(NEO4J_SHELL.begin(), PLAIN_FORMAT.begin()).replace(NEO4J_SHELL.commit(), PLAIN_FORMAT.commit())
                .replace(NEO4J_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

        static final String EXPECTED_NEO4J_MERGE = EXPECTED_NODES_MERGE + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS_MERGE + EXPECTED_CLEAN_UP;

        static final String EXPECTED_ONLY_SCHEMA_CYPHER_SHELL = EXPECTED_ONLY_SCHEMA_NEO4J_SHELL.replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit()).replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait()) + EXPECTED_INDEXES_AWAIT;


        static final String EXPECTED_NODES_COMPOUND_CONSTRAINT = String.format("BEGIN%n" +
                "CREATE (:`Person` {`name`:\"John\", `surname`:\"Snow\"});%n" +
                "CREATE (:`Person` {`name`:\"Matt\", `surname`:\"Jackson\"});%n" +
                "CREATE (:`Person` {`name`:\"Jenny\", `surname`:\"White\"});%n" +
                "CREATE (:`Person` {`name`:\"Susan\", `surname`:\"Brown\"});%n" +
                "CREATE (:`Person` {`name`:\"Tom\", `surname`:\"Taylor\"});%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA_COMPOUND_CONSTRAINT = String.format("BEGIN%n" +
                "CREATE CONSTRAINT ON (node:`Person`) ASSERT (node.`name`, node.`surname`) IS NODE KEY;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_RELATIONSHIP_COMPOUND_CONSTRAINT = String.format(("BEGIN%n" +
                "MATCH (n1:`Person`{`surname`:\"Snow\", `name`:\"John\"}), (n2:`Person`{`surname`:\"Jackson\", `name`:\"Matt\"}) CREATE (n1)-[r:`KNOWS`]->(n2);%n" +
                "COMMIT%n"));

        static final String EXPECTED_INDEX_AWAIT_COMPOUND_CONSTRAINT =  String.format("CALL db.awaitIndex(':`Person`(`name`,`surname`)');%n");

        static final String EXPECTED_NEO4J_SHELL_WITH_COMPOUND_CONSTRAINT = String.format("BEGIN%n" +
                "CREATE CONSTRAINT ON (node:Person) ASSERT (node.name, node.surname) IS NODE KEY;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] AS row%n" +
                "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] AS row%n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_SHELL_WITH_COMPOUND_CONSTRAINT = String.format(":begin%n" +
                "CREATE CONSTRAINT ON (node:Person) ASSERT (node.name, node.surname) IS NODE KEY;%n" +
                ":commit%n" +
                "CALL db.awaitIndexes(300);%n" +
                ":begin%n" +
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] AS row%n" +
                "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n" +
                ":commit%n" +
                ":begin%n" +
                "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] AS row%n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                ":commit%n");

        static final String EXPECTED_PLAIN_FORMAT_WITH_COMPOUND_CONSTRAINT = String.format("CREATE CONSTRAINT ON (node:Person) ASSERT (node.name, node.surname) IS NODE KEY;%n" +
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] AS row%n" +
                "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] AS row%n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n");
    }

}
