package apoc.export.cypher;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;

import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.*;
import static apoc.export.util.ExportFormat.*;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypherTest {

    private static final Map<String, Object> exportConfig = Collections.singletonMap("separateFiles", true);
    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public TestName testName = new TestName();

    private static final String OPTIMIZED = "Optimized";

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true").newGraphDatabase();
        TestUtil.registerProcedure(db, ExportCypher.class, Graphs.class);
        if (testName.getMethodName().endsWith(OPTIMIZED)) {
            db.execute("CREATE INDEX ON :Foo(name)").close();
            db.execute("CREATE INDEX ON :Bar(first_name, last_name)").close();
            db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE").close();
            db.execute("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar:Person {age:12}),(d:Bar {age:12})," +
                    " (t:Foo {name:'foo2', born:date('2017-09-29')})-[:KNOWS {since:2015}]->(e:Bar {name:'bar2',age:44}),({age:99})").close();
        } else {
            db.execute("CREATE INDEX ON :Foo(name)").close();
            db.execute("CREATE INDEX ON :Bar(first_name, last_name)").close();
            db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE").close();
            db.execute("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})").close();
        }
    }

    @After
    public void tearDown() {
        db.shutdown();
    }


    @Test
    public void testExportAllCypherResults() {
        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null,null)", (r) -> {
            assertResults(null, r, "database");
            assertEquals(EXPECTED_NEO4J_SHELL, r.get("cypherStatements"));
        });
    }

    @Test
    public void testExportAllCypherStreaming() {
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.cypher.all(null,{streamStatements:true,batchSize:3})", (res) -> {
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
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},null)", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(output));
    }

    @Test
    public void testExportAllCypherForCypherShell() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{config})",
                map("file", output.getAbsolutePath(), "config", Util.map("format", "cypher-shell")), (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_CYPHER_SHELL, readFile(output));
    }

    @Test
    public void testExportQueryCypherForNeo4j() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(output));
    }

    private static String readFile(File output) throws FileNotFoundException {
        return new Scanner(output).useDelimiter("\\Z").next() + String.format("%n");
    }

    @Test
    public void testExportGraphCypher() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},null) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(output));
    }

    // -- Separate files tests -- //
    @Test
    public void testExportAllCypherNodes() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_NODES, readFile(new File(directory, "all.nodes.cypher")));
    }

    @Test
    public void testExportAllCypherRelationships() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile(new File(directory, "all.relationships.cypher")));
    }

    @Test
    public void testExportAllCypherSchema() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_SCHEMA, readFile(new File(directory, "all.schema.cypher")));
    }

    @Test
    public void testExportAllCypherCleanUp() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_CLEAN_UP, readFile(new File(directory, "all.cleanup.cypher")));
    }

    @Test
    public void testExportGraphCypherNodes() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig), (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_NODES, readFile(new File(directory, "graph.nodes.cypher")));
    }

    @Test
    public void testExportGraphCypherRelationships() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile(new File(directory, "graph.relationships.cypher")));
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_SCHEMA, readFile(new File(directory, "graph.schema.cypher")));
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_CLEAN_UP, readFile(new File(directory, "graph.cleanup.cypher")));
    }

    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(6L, r.get("properties"));
        assertEquals(output == null ? null : output.getAbsolutePath(), r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportQueryCypherPlainFormat() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "plain")), (r) -> {
                });
        assertEquals(EXPECTED_PLAIN, readFile(output));
    }

    @Test
    public void testExportQueryCypherFormatUpdateAll() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell", "cypherFormat", "updateAll")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_MERGE, readFile(output));
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell", "cypherFormat", "addStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_MERGE_ON_CREATE_SET + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP_EMPTY, readFile(output));
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell", "cypherFormat", "updateStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_EMPTY + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET + EXPECTED_CLEAN_UP_EMPTY, readFile(output));
    }

    @Test
    public void testExportSchemaCypher() throws Exception {
        File output = new File(directory, "onlySchema.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig), (r) -> {
        });
        assertEquals(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL, readFile(new File(directory, "onlySchema.cypher")));
    }

    @Test
    public void testExportSchemaCypherShell() throws Exception {
        File output = new File(directory, "onlySchema.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", Util.map("format", "cypher-shell")), (r) -> {
        });
        assertEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, readFile(new File(directory, "onlySchema.cypher")));
    }

    @Test
    public void testExportCypherNodePoint() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo'," +
                "place2d:point({ x: 2.3, y: 4.5 })," +
                "place3d1:point({ x: 2.3, y: 4.5 , z: 1.2})})" +
                "-[:FRIEND_OF {place2d:point({ longitude: 56.7, latitude: 12.78 })}]->" +
                "(:Bar {place3d:point({ longitude: 12.78, latitude: 56.7, height: 100 })})").close();
        File output = new File(directory, "temporalPoint.cypher");
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_CYPHER_POINT, readFile(output));
    }

    @Test
    public void testExportCypherNodeDate() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "date:date('2018-10-30'), " +
                "datetime:datetime('2018-10-30T12:50:35.556+0100'), " +
                "localTime:localdatetime('20181030T19:32:24')})" +
                "-[:FRIEND_OF {date:date('2018-10-30')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556')})").close();
        File output = new File(directory, "temporalDate.cypher");
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_CYPHER_DATE, readFile(output));
    }

    @Test
    public void testExportCypherNodeTime() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "local:localtime('12:50:35.556')," +
                "t:time('125035.556+0100')})" +
                "-[:FRIEND_OF {t:time('125035.556+0100')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556+0100')})").close();
        File output = new File(directory, "temporalTime.cypher");
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_CYPHER_TIME, readFile(output));
    }

    @Test
    public void testExportCypherNodeDuration() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "duration:duration('P5M1.5D')})" +
                "-[:FRIEND_OF {duration:duration('P5M1.5D')}]->" +
                "(:Bar {duration:duration('P5M1.5D')})").close();
        File output = new File(directory, "temporalDuration.cypher");
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_CYPHER_DURATION, readFile(output));
    }

    @Test
    public void testExportWithAscendingLabels() throws FileNotFoundException {
        db.execute("CREATE (f:User:User1:User0:User12 {name:'Alan'})").close();
        File output = new File(directory, "ascendingLabels.cypher");
        String query = "MATCH (f:User) WHERE f.name='Alan' RETURN f";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_CYPHER_LABELS_ASCENDEND, readFile(output));
    }

    @Test
    public void testExportAllCypherDefaultOptimized() throws Exception {
        File output = new File(directory, "allDefaultOptimized.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{useOptimizations:true})", map("file", output.getAbsolutePath()), (r) -> assertResultsOptimized(output, r, "database"));
        assertEquals(EXPECTED_NEO4J_OPTIMIZED, readFile(output));
    }

    @Test
    public void testExportAllCypherCypherShellOptimized() throws Exception {
        File output = new File(directory, "allCypherShellOptimized.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'cypher-shell', useOptimizations:true})", map("file", output.getAbsolutePath()), (r) -> assertResultsOptimized(output, r, "database"));
        assertEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED, readFile(output));
    }

    @Test
    public void testExportAllCypherPlainOptimized() throws Exception {
        File output = new File(directory, "allPlainOptimized.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'plain', useOptimizations:true})", map("file", output.getAbsolutePath()), (r) -> assertResultsOptimized(output, r, "database"));
        assertEquals(EXPECTED_PLAIN_OPTIMIZED, readFile(output));
    }

    private void assertResultsOptimized(File output, Map<String, Object> r, final String source) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(output == null ? null : output.getAbsolutePath(), r.get("file"));
        assertEquals(source + ": nodes(7), rels(2)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    static class ExportCypherResults {

        static final String EXPECTED_NODES = String.format("BEGIN%n" +
                "CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`born`:date('2018-10-31'), `name`:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
                "CREATE (:`Bar` {`age`:42, `name`:\"bar\"});%n" +
                "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`age`:12, `UNIQUE IMPORT ID`:2});%n" +
                "COMMIT%n");

        private static final String EXPECTED_NODES_MERGE = String.format("BEGIN%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) SET n.`name`=\"foo\", n.`born`=date('2018-10-31'), n:`Foo`;%n" +
                "MERGE (n:`Bar`{`name`:\"bar\"}) SET n.`age`=42;%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) SET n.`age`=12, n:`Bar`;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_MERGE_ON_CREATE_SET =
                EXPECTED_NODES_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        static final String EXPECTED_NODES_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA = String.format("BEGIN%n" +
                "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
                "CREATE INDEX ON :`Foo`(`name`);%n" +
                "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        private static final String EXPECTED_INDEXES_AWAIT = String.format("CALL db.awaitIndex(':`Foo`(`name`)');%n" +
                "CALL db.awaitIndex(':`Bar`(`first_name`,`last_name`)');%n" +
                "CALL db.awaitIndex(':`Bar`(`name`)');%n");

        static final String EXPECTED_RELATIONSHIPS = String.format("BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) CREATE (n1)-[r:`KNOWS` {`since`:2016}]->(n2);%n" +
                "COMMIT%n");

        private static final String EXPECTED_RELATIONSHIPS_MERGE = String.format("BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) MERGE (n1)-[r:`KNOWS`]->(n2) SET r.`since`=2016;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET =
                EXPECTED_RELATIONSHIPS_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        static final String EXPECTED_CLEAN_UP = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CLEAN_UP_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "COMMIT%n");

        static final String EXPECTED_ONLY_SCHEMA_NEO4J_SHELL = String.format("BEGIN%n" +
                "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
                "CREATE INDEX ON :`Foo`(`name`);%n" +
                "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_CYPHER_POINT = String.format("BEGIN%n" +
                "CREATE (:`Test`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `place2d`:point({x: 2.3, y: 4.5, crs: 'cartesian'}), `place3d1`:point({x: 2.3, y: 4.5, z: 1.2, crs: 'cartesian-3d'}), `UNIQUE IMPORT ID`:20});%n" +
                "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`place3d`:point({x: 12.78, y: 56.7, z: 100.0, crs: 'wgs-84-3d'}), `UNIQUE IMPORT ID`:21});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
                "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:`FRIEND_OF` {`place2d`:point({x: 56.7, y: 12.78, crs: 'wgs-84'})}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_DATE = String.format("BEGIN%n" +
                "CREATE (:`Test`:`UNIQUE IMPORT LABEL` {`date`:date('2018-10-30'), `datetime`:datetime('2018-10-30T12:50:35.556+01:00'), `localTime`:localdatetime('2018-10-30T19:32:24'), `name`:\"foo\", `UNIQUE IMPORT ID`:20});%n" +
                "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`datetime`:datetime('2018-10-30T12:50:35.556Z'), `UNIQUE IMPORT ID`:21});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
                "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:`FRIEND_OF` {`date`:date('2018-10-30')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_TIME = String.format("BEGIN%n" +
                "CREATE (:`Test`:`UNIQUE IMPORT LABEL` {`local`:localtime('12:50:35.556'), `name`:\"foo\", `t`:time('12:50:35.556+01:00'), `UNIQUE IMPORT ID`:20});%n" +
                "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`datetime`:datetime('2018-10-30T12:50:35.556+01:00'), `UNIQUE IMPORT ID`:21});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
                "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:`FRIEND_OF` {`t`:time('12:50:35.556+01:00')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_DURATION = String.format("BEGIN%n" +
                "CREATE (:`Test`:`UNIQUE IMPORT LABEL` {`duration`:duration('P5M1DT12H'), `name`:\"foo\", `UNIQUE IMPORT ID`:20});%n" +
                "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`duration`:duration('P5M1DT12H'), `UNIQUE IMPORT ID`:21});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
                "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:`FRIEND_OF` {`duration`:duration('P5M1DT12H')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_LABELS_ASCENDEND = String.format("BEGIN%n" +
                "CREATE (:`User`:`User0`:`User1`:`User12`:`UNIQUE IMPORT LABEL` {`name`:\"Alan\", `UNIQUE IMPORT ID`:20});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{`UNIQUE IMPORT ID`: 0, properties: {`born`:date('2018-10-31'), `name`:\"foo\"}}, {`UNIQUE IMPORT ID`: 4, properties: {`born`:date('2017-09-29'), `name`:\"foo2\"}}] as row %n" +
                "MERGE (n:`Foo`:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.`UNIQUE IMPORT ID`}) SET n += row.properties;%n" +
                "UNWIND [{`name`: \"bar\", properties: {`age`:42, `name`:\"bar\"}}, {`name`: \"bar2\", properties: {`age`:44, `name`:\"bar2\"}}] as row %n" +
                "MERGE (n:`Bar`{`name`: row.`name`}) SET n += row.properties;%n" +
                "UNWIND [{`UNIQUE IMPORT ID`: 2, properties: {`age`:12}}] as row %n" +
                "MERGE (n:`Bar`:`Person`:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.`UNIQUE IMPORT ID`}) SET n += row.properties;%n" +
                "UNWIND [{`UNIQUE IMPORT ID`: 6, properties: {`age`:99}}] as row %n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.`UNIQUE IMPORT ID`}) SET n += row.properties;%n" +
                "UNWIND [{`UNIQUE IMPORT ID`: 3, properties: {`age`:12}}] as row %n" +
                "MERGE (n:`Bar`:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.`UNIQUE IMPORT ID`}) SET n += row.properties;%n" +
                "%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA_OPTIMIZED = String.format("BEGIN%n" +
                "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
                "CREATE INDEX ON :`Foo`(`name`);%n" +
                "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_RELATIONSHIPS_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{start: {`UNIQUE IMPORT ID`: 0}, end: {`name`: \"bar\"}, properties: {`since`:2016}}, {start: {`UNIQUE IMPORT ID`: 4}, end: {`name`: \"bar2\"}, properties: {`since`:2015}}] as row %n" +
                "MATCH (start:`Foo`:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start.`UNIQUE IMPORT ID`}), (end:`Bar`{`name`: row.end.`name`})%n" +
                "MERGE (start)-[r:`KNOWS`]->(end) SET r += row.properties;%n" +
                "%n" +
                "COMMIT%n");

        static final String EXPECTED_INDEXES_AWAIT_OPTIMIZED = String.format("CALL db.awaitIndex(':`Foo`(`name`)');%n" +
                "CALL db.awaitIndex(':`Bar`(`first_name`,`last_name`)');%n" +
                "CALL db.awaitIndex(':`Bar`(`name`)');%n");

        static final String DROP_UNIQUE_OPTIMIZED = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_NEO4J_OPTIMIZED = EXPECTED_NODES_OPTIMIZED + EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_NEO4J_SHELL_OPTIMIZED = EXPECTED_NODES_OPTIMIZED + EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_CYPHER_SHELL_OPTIMIZED = EXPECTED_NEO4J_SHELL_OPTIMIZED
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_PLAIN_OPTIMIZED = EXPECTED_NEO4J_SHELL_OPTIMIZED
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

    }

}