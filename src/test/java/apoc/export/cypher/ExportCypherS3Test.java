package apoc.export.cypher;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.s3.S3TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CLEAN_UP;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CLEAN_UP_EMPTY;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_DATE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_DURATION;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_LABELS_ASCENDEND;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_POINT;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_SHELL;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_SHELL_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_TIME;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_INDEXES_AWAIT;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NEO4J_MERGE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NEO4J_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NEO4J_SHELL;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NODES;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NODES_EMPTY;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NODES_MERGE_ON_CREATE_SET;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NODES_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_ONLY_SCHEMA_CYPHER_SHELL;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_ONLY_SCHEMA_NEO4J_SHELL;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_PLAIN;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_RELATIONSHIPS;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_RELATIONSHIPS_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_RELATIONSHIPS_PARAMS_ODD;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_EMPTY;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_UPDATE_ALL_UNWIND;
import static apoc.export.util.ExportFormat.CYPHER_SHELL;
import static apoc.export.util.ExportFormat.NEO4J_SHELL;
import static apoc.util.TestUtil.isCI;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class ExportCypherS3Test {
    private static S3TestUtil s3TestUtil;
    private static final Map<String, Object> exportConfig = map("useOptimizations", map("type", "none"), "separateFiles", true, "format", "neo4j-admin");
    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public TestName testName = new TestName();

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

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ExportCypher.class, Graphs.class);
        db.execute("CREATE INDEX ON :Bar(first_name, last_name)").close();
        db.execute("CREATE INDEX ON :Foo(name)").close();
        db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE").close();
        if (testName.getMethodName().endsWith(ExportCypherTest.OPTIMIZED)) {
            db.execute("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar:Person {age:12}),(d:Bar {age:12})," +
                    " (t:Foo {name:'foo2', born:date('2017-09-29')})-[:KNOWS {since:2015}]->(e:Bar {name:'bar2',age:44}),({age:99})").close();
        } else if(testName.getMethodName().endsWith(ExportCypherTest.ODD)) {
            db.execute("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})," +
                    "(t:Foo {name:'foo2', born:date('2017-09-29')})," +
                    "(g:Foo {name:'foo3', born:date('2016-03-12')})," +
                    "(b:Bar {name:'bar',age:42})," +
                    "(c:Bar {age:12})," +
                    "(d:Bar {age:4})," +
                    "(e:Bar {name:'bar2',age:44})," +
                    "(f)-[:KNOWS {since:2016}]->(b)").close();
        } else {
            db.execute("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})").close();
        }
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    // -- Whole file test -- //
    @Test
    public void testExportAllCypherDefault() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{useOptimizations: { type: 'none'}, format: 'neo4j-shell'})",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_NEO4J_SHELL);
    }

    @Test
    public void testExportAllCypherForCypherShell() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$config)",
                map("s3", s3Url, "config", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_CYPHER_SHELL);
    }

    @Test
    public void testExportQueryCypherForNeo4j() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")), (r) -> {
                });
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_NEO4J_SHELL);
    }

    @Test
    public void testExportGraphCypher() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> assertResults(s3Url, r, "graph"));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_NEO4J_SHELL);
    }

    // -- Separate files tests -- //
    @Test
    public void testExportAllCypherNodes() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, "all.nodes.cypher", EXPECTED_NODES);
    }

    @Test
    public void testExportAllCypherRelationships() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, "all.relationships.cypher", EXPECTED_RELATIONSHIPS);
    }

    @Test
    public void testExportAllCypherSchema() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, "all.schema.cypher", EXPECTED_SCHEMA);
    }

    @Test
    public void testExportAllCypherCleanUp() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        s3TestUtil.verifyUpload(directory, "all.cleanup.cypher", EXPECTED_CLEAN_UP);
    }

    @Test
    public void testExportGraphCypherNodes() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        s3TestUtil.verifyUpload(directory, "graph.nodes.cypher", EXPECTED_NODES);
    }

    @Test
    public void testExportGraphCypherRelationships() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source, format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        s3TestUtil.verifyUpload(directory, "graph.relationships.cypher", EXPECTED_RELATIONSHIPS);
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        s3TestUtil.verifyUpload(directory, "graph.schema.cypher", EXPECTED_SCHEMA);
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        s3TestUtil.verifyUpload(directory, "graph.cleanup.cypher", EXPECTED_CLEAN_UP);
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
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "plain")), (r) -> {
                });
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_PLAIN);
    }

    @Test
    public void testExportQueryCypherFormatUpdateAll() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateAll")), (r) -> {
                });
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_NEO4J_MERGE);
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "addStructure")), (r) -> {
                });
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_NODES_MERGE_ON_CREATE_SET + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP_EMPTY);
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateStructure")), (r) -> {
                });
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_NODES_EMPTY + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET + EXPECTED_CLEAN_UP_EMPTY);
    }

    @Test
    public void testExportSchemaCypher() throws Exception {
        String fileName = "onlySchema.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($s3,$exportConfig)", map("s3", s3Url, "exportConfig", exportConfig), (r) -> {
        });
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_ONLY_SCHEMA_NEO4J_SHELL);
    }

    @Test
    public void testExportSchemaCypherShell() throws Exception {
        String fileName = "onlySchema.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> {});
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_ONLY_SCHEMA_CYPHER_SHELL);
    }

    @Test
    public void testExportCypherNodePoint() throws IOException {
        db.execute("CREATE (f:Test {name:'foo'," +
                "place2d:point({ x: 2.3, y: 4.5 })," +
                "place3d1:point({ x: 2.3, y: 4.5 , z: 1.2})})" +
                "-[:FRIEND_OF {place2d:point({ longitude: 56.7, latitude: 12.78 })}]->" +
                "(:Bar {place3d:point({ longitude: 12.78, latitude: 56.7, height: 100 })})");
        String fileName = "temporalPoint.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_CYPHER_POINT);
    }

    @Test
    public void testExportCypherNodeDate() throws IOException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "date:date('2018-10-30'), " +
                "datetime:datetime('2018-10-30T12:50:35.556+0100'), " +
                "localTime:localdatetime('20181030T19:32:24')})" +
                "-[:FRIEND_OF {date:date('2018-10-30')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556')})");
        String fileName = "temporalDate.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_CYPHER_DATE);
    }

    @Test
    public void testExportCypherNodeTime() throws IOException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "local:localtime('12:50:35.556')," +
                "t:time('125035.556+0100')})" +
                "-[:FRIEND_OF {t:time('125035.556+0100')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556+0100')})");
        String fileName = "temporalTime.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_CYPHER_TIME);
    }

    @Test
    public void testExportCypherNodeDuration() throws IOException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "duration:duration('P5M1.5D')})" +
                "-[:FRIEND_OF {duration:duration('P5M1.5D')}]->" +
                "(:Bar {duration:duration('P5M1.5D')})");
        String fileName = "temporalDuration.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_CYPHER_DURATION);
    }

    @Test
    public void testExportWithAscendingLabels() throws IOException {
        db.execute("CREATE (f:User:User1:User0:User12 {name:'Alan'})");
        String fileName = "ascendingLabels.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        String query = "MATCH (f:User) WHERE f.name='Alan' RETURN f";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_CYPHER_LABELS_ASCENDEND);
    }

    @Test
    public void testExportAllCypherDefaultWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, format: 'neo4j-shell'})", map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE);
    }

    @Test
    public void testExportAllCypherDefaultOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3, $exportConfig)", map("s3", s3Url, "exportConfig", map("format", "neo4j-shell")),
                (r) -> assertResultsOptimized(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_NEO4J_OPTIMIZED);
    }

    @Test
    public void testExportAllCypherDefaultSeparatedFilesOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3, $exportConfig)",
                map("s3", s3Url, "exportConfig", map("separateFiles", true, "format", "neo4j-shell")),
                (r) -> assertResultsOptimized(s3Url, r));
        s3TestUtil.verifyUpload(directory, "allDefaultOptimized.nodes.cypher", EXPECTED_NODES_OPTIMIZED);
        s3TestUtil.verifyUpload(directory, "allDefaultOptimized.relationships.cypher", EXPECTED_RELATIONSHIPS_OPTIMIZED);
        s3TestUtil.verifyUpload(directory, "allDefaultOptimized.schema.cypher", EXPECTED_SCHEMA_OPTIMIZED);
        s3TestUtil.verifyUpload(directory, "allDefaultOptimized.cleanup.cypher", EXPECTED_CLEAN_UP);
    }

    @Test
    public void testExportAllCypherCypherShellWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE);
    }

    @Test
    public void testExportAllCypherCypherShellOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell'})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_CYPHER_SHELL_OPTIMIZED);
    }

    @Test
    public void testExportAllCypherPlainWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE);
    }

    @Test
    public void testExportAllCypherPlainAddStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainAddStructureOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', cypherFormat: 'addStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url), (r) -> assertResultsOptimized(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND);
    }

    @Test
    public void testExportAllCypherPlainUpdateStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateStructureOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', cypherFormat: 'updateStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url), (r) -> {
                    assertEquals(0L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(2L, r.get("properties"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                });
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND);
    }

    @Test
    public void testExportAllCypherPlainUpdateAllWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateAllOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', cypherFormat: 'updateAll', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url), (r) -> assertResultsOptimized(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_UPDATE_ALL_UNWIND);
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND);
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("s3", s3Url), (r) -> assertResultsOdd(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD);
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:2})",
                map("s3", s3Url),
                (r) -> assertResultsOdd(s3Url, r));
        s3TestUtil.verifyUpload(directory, fileName, EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD);
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddBatchSizeOddDataset() throws Exception {
        db.execute("CREATE (:Bar {name:'bar3',age:35}), (:Bar {name:'bar4',age:36})");
        String fileName = "allPlainOddNew.cypher";
        String s3Url = s3TestUtil.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:3})",
                map("s3", s3Url),
                (r) -> {});
        db.execute("MATCH (n:Bar {name:'bar3',age:35}), (n1:Bar {name:'bar4',age:36}) DELETE n, n1");
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
        s3TestUtil.verifyUpload(directory, fileName, expected);
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
}
