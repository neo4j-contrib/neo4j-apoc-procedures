package apoc.export.cypher;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static apoc.export.util.ExportFormat.CYPHER_SHELL;
import static apoc.export.util.ExportFormat.NEO4J_SHELL;
import static apoc.export.util.ExportFormat.PLAIN_FORMAT;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypherTest {

    private static final String EXPECTED_NODES = String.format("BEGIN%n" +
            "CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
            "CREATE (:`Bar` {`age`:42, `name`:\"bar\"});%n" +
            "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`age`:12, `UNIQUE IMPORT ID`:2});%n" +
            "COMMIT%n");

    private static final String EXPECTED_NODES_MERGE = String.format("BEGIN%n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) SET n.`name`=\"foo\", n:`Foo`;%n" +
            "MERGE (n:`Bar`{`name`:\"bar\"}) SET n.`age`=42;%n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) SET n.`age`=12, n:`Bar`;%n" +
            "COMMIT%n");

    private static final String EXPECTED_NODES_MERGE_ON_CREATE_SET =
            EXPECTED_NODES_MERGE.replaceAll(" SET ", " ON CREATE SET ");

    private static final String EXPECTED_NODES_EMPTY = String.format("BEGIN%n" +
            "COMMIT%n");

    private static final String EXPECTED_SCHEMA = String.format("BEGIN%n" +
            "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
            "CREATE INDEX ON :`Foo`(`name`);%n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
            "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n");

    private static final String EXPECTED_SCHEMA_EMPTY = String.format("BEGIN%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n");

    private static final String EXPECTED_INDEXES_AWAIT = String.format("CALL db.awaitIndex(':`Foo`(`name`)');%n" +
            "CALL db.awaitIndex(':`Bar`(`first_name`,`last_name`)');%n" +
            "CALL db.awaitIndex(':`Bar`(`name`)');%n");

    private static final String EXPECTED_RELATIONSHIPS = String.format("BEGIN%n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) CREATE (n1)-[r:`KNOWS` {`since`:2016}]->(n2);%n" +
            "COMMIT%n");

    private static final String EXPECTED_RELATIONSHIPS_MERGE = String.format("BEGIN%n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) MERGE (n1)-[r:`KNOWS`]->(n2) SET r.`since`=2016;%n" +
            "COMMIT%n");

    private static final String EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET =
            EXPECTED_RELATIONSHIPS_MERGE.replaceAll(" SET ", " ON CREATE SET ");

    private static final String EXPECTED_CLEAN_UP = String.format("BEGIN%n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n");

    private static final String EXPECTED_CLEAN_UP_EMPTY = String.format("BEGIN%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "COMMIT%n");

    private static final String EXPECTED_ONLY_SCHEMA_NEO4J_SHELL = String.format("BEGIN%n" +
            "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
            "CREATE INDEX ON :`Foo`(`name`);%n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n");

    private static final String EXPECTED_NEO4J_SHELL = EXPECTED_NODES + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP;

    private static final String EXPECTED_CYPHER_SHELL = EXPECTED_NEO4J_SHELL
            .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
            .replace(NEO4J_SHELL.commit(),CYPHER_SHELL.commit())
            .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
            .replace(NEO4J_SHELL.schemaAwait(),CYPHER_SHELL.schemaAwait());

    private static final String EXPECTED_PLAIN = EXPECTED_NEO4J_SHELL
            .replace(NEO4J_SHELL.begin(), PLAIN_FORMAT.begin()).replace(NEO4J_SHELL.commit(), PLAIN_FORMAT.commit())
            .replace(NEO4J_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

    private static final String EXPECTED_NEO4J_MERGE = EXPECTED_NODES_MERGE + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS_MERGE + EXPECTED_CLEAN_UP;

    private static final String EXPECTED_ONLY_SCHEMA_CYPHER_SHELL = EXPECTED_ONLY_SCHEMA_NEO4J_SHELL.replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
            .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit()).replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait()) + EXPECTED_INDEXES_AWAIT;

    private static final Map<String, Object> exportConfig = Collections.singletonMap("separateFiles", true);
    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @BeforeClass public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true").newGraphDatabase();
        TestUtil.registerProcedure(db, ExportCypher.class, Graphs.class);
        db.execute("CREATE INDEX ON :Foo(name)").close();
        db.execute("CREATE INDEX ON :Bar(first_name, last_name)").close();
        db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE").close();
        db.execute("CREATE (f:Foo {name:'foo'})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})").close();
    }

    @AfterClass public static void tearDown() {
        db.shutdown();
    }


    @Test public void testExportAllCypherResults() throws Exception {
        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null,null)", (r) -> {
            System.out.println(r);
            assertResults(null, r, "database");
            assertEquals(EXPECTED_NEO4J_SHELL,r.get("cypherStatements"));
        });
    }

    @Test public void testExportAllCypherStreaming() throws Exception {
        StringBuilder sb=new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.cypher.all(null,{streamStatements:true,batchSize:3})", (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(4L, r.get("properties"));
            assertEquals(null, r.get("file"));
            assertEquals("cypher", r.get("format"));
            assertEquals(true, ((long) r.get("time")) >= 0);
            sb.append(r.get("cypherStatements"));
            r = res.next();
            System.out.println(r);
            assertEquals(3L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(1L, r.get("relationships"));
            assertEquals(5L, r.get("properties"));
            assertEquals(true, ((long) r.get("time")) >= 0);
            sb.append(r.get("cypherStatements"));
        });
        assertEquals(EXPECTED_NEO4J_SHELL.replace("LIMIT 20000","LIMIT 3"),sb.toString());
    }

    // -- Whole file test -- //
    @Test public void testExportAllCypherDefault() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},null)", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(output));
    }

    @Test public void testExportAllCypherForCypherShell() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{config})",
                map("file", output.getAbsolutePath(), "config", Util.map("format", "cypher-shell")), (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_CYPHER_SHELL, readFile(output));
    }

    @Test public void testExportQueryCypherForNeo4j() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(output));
    }

    public static String readFile(File output) throws FileNotFoundException {
        return new Scanner(output).useDelimiter("\\Z").next() + String.format("%n");
    }

    @Test public void testExportGraphCypher() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},null) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(output));
    }

    // -- Separate files tests -- //
    @Test public void testExportAllCypherNodes() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_NODES, readFile(new File(directory, "all.nodes.cypher")));
    }

    @Test public void testExportAllCypherRelationships() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile(new File(directory, "all.relationships.cypher")));
    }

    @Test public void testExportAllCypherSchema() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_SCHEMA, readFile(new File(directory, "all.schema.cypher")));
    }

    @Test public void testExportAllCypherCleanUp() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_CLEAN_UP, readFile(new File(directory, "all.cleanup.cypher")));
    }

    @Test public void testExportGraphCypherNodes() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig), (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_NODES, readFile(new File(directory, "graph.nodes.cypher")));
    }

    @Test public void testExportGraphCypherRelationships() throws Exception {
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
        assertEquals(5L, r.get("properties"));
        assertEquals(output==null ? null : output.getAbsolutePath(), r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertEquals(true, ((long) r.get("time")) >= 0);
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
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell","cypherFormat","updateAll")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_MERGE, readFile(output));
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell","cypherFormat","addStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_MERGE_ON_CREATE_SET + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP_EMPTY, readFile(output));
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell","cypherFormat","updateStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_EMPTY + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET + EXPECTED_CLEAN_UP_EMPTY, readFile(output));
    }

    @Test public void testExportSchemaCypher() throws Exception {
        File output = new File(directory, "onlySchema.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig), (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL, readFile(new File(directory, "onlySchema.cypher")));
    }

    @Test public void testExportSchemaCypherShell() throws Exception {
        File output = new File(directory, "onlySchema.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", Util.map("format", "cypher-shell")), (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, readFile(new File(directory, "onlySchema.cypher")));
    }
}
