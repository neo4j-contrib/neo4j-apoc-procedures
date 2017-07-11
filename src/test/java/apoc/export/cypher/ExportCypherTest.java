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

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypherTest {

    private interface ExpectedOutputs {
        String nodes();
        String schema();
        String relationships();
        String cleanup();
        default String all()
        {
            return nodes() + schema() + relationships() + cleanup();
        }
    }

    private static class CypherOutput implements ExpectedOutputs
    {
        @Override
        public String nodes() {
            return String.format(":BEGIN%n" +
                    "CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
                    "CREATE (:`Bar` {`name`:\"bar\", `age`:42});%n" +
                    "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`age`:12, `UNIQUE IMPORT ID`:2});%n" +
                    ":COMMIT%n");
        }

        @Override
        public String schema() {
            return String.format(":BEGIN%n" +
                    "CREATE INDEX ON :`Foo`(`name`);%n" +
                    "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                    "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                    ":COMMIT%n" +
                    "CALL db.awaitIndex(':`Foo`(`name`)');%n" +
                    "CALL db.awaitIndex(':`Bar`(`name`)');%n");
        }

        @Override
        public String relationships() {
            return String.format(":BEGIN%n" +
                    "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) CREATE (n1)-[:`KNOWS`]->(n2);%n" +
                    ":COMMIT%n");
        }

        @Override
        public String cleanup() {
            return String.format(":BEGIN%n" +
                    "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                    ":COMMIT%n" +
                    ":BEGIN%n" +
                    "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                    ":COMMIT%n");
        }
    }
    private static ExpectedOutputs cypherOutput = new CypherOutput();

    private static class Neo4jShellOutput implements ExpectedOutputs
    {
        @Override
        public String nodes() {
            return String.format("begin%n" +
                    "CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
                    "CREATE (:`Bar` {`name`:\"bar\", `age`:42});%n" +
                    "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`age`:12, `UNIQUE IMPORT ID`:2});%n" +
                    "commit%n");
        }

        @Override
        public String schema() {
            return String.format("begin%n" +
                    "CREATE INDEX ON :`Foo`(`name`);%n" +
                    "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                    "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                    "commit%n" +
                    "schema await%n");
        }

        @Override
        public String relationships() {
            return String.format("begin%n" +
                    "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) CREATE (n1)-[:`KNOWS`]->(n2);%n" +
                    "commit%n");
        }

        @Override
        public String cleanup() {
            return String.format("begin%n" +
                    "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                    "commit%n" +
                    "begin%n" +
                    "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
                    "commit%n");
        }
    }
    private static ExpectedOutputs neo4jShellOutput = new Neo4jShellOutput();

    private static class PlainOutput implements ExpectedOutputs
    {
        @Override
        public String nodes() {
            return String.format(
                    "CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
                    "CREATE (:`Bar` {`name`:\"bar\", `age`:42});%n" +
                    "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`age`:12, `UNIQUE IMPORT ID`:2});%n");
        }

        @Override
        public String schema() {
            return String.format(
                    "CREATE INDEX ON :`Foo`(`name`);%n" +
                    "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                    "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n");
        }

        @Override
        public String relationships() {
            return String.format(
                    "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) CREATE (n1)-[:`KNOWS`]->(n2);%n");
        }

        @Override
        public String cleanup() {
            return String.format(
                    "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                    "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n");
        }
    }
    private static ExpectedOutputs plainOutput = new PlainOutput();

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
        db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE").close();
        db.execute("CREATE (f:Foo {name:'foo'})-[:KNOWS]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})").close();
    }

    @AfterClass public static void tearDown() {
        db.shutdown();
    }

    // -- Whole file test -- //
    @Test public void testExportAllCypherDefault() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},null)", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "database"));
        assertEquals(cypherOutput.all(), readFile(output));
    }

    @Test public void testExportAllCypherForCypherShell() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{config})",
                map("file", output.getAbsolutePath(), "config", Util.map("format", "cypher-shell")), (r) -> assertResults(output, r, "database"));
        assertEquals(cypherOutput.all(), readFile(output));
    }

    @Test public void testExportQueryCypherForNeo4j() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(neo4jShellOutput.all(), readFile(output));
    }

    public static String readFile(File output) throws FileNotFoundException {
        return new Scanner(output).useDelimiter("\\Z").next()+String.format("%n");
    }

    @Test public void testExportGraphCypher() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},null) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "graph"));
        assertEquals(cypherOutput.all(), readFile(output));
    }

    // -- Separate files tests -- //
    @Test public void testExportAllCypherNodes() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(cypherOutput.nodes(), readFile(new File(directory, "all.nodes.cypher")));
    }

    @Test public void testExportAllCypherRelationships() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(cypherOutput.relationships(), readFile(new File(directory, "all.relationships.cypher")));
    }

    @Test public void testExportAllCypherSchema() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(cypherOutput.schema(), readFile(new File(directory, "all.schema.cypher")));
    }

    @Test public void testExportAllCypherCleanUp() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(cypherOutput.cleanup(), readFile(new File(directory, "all.cleanup.cypher")));
    }

    @Test public void testExportGraphCypherNodes() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig), (r) -> assertResults(output, r, "graph"));
        assertEquals(cypherOutput.nodes(), readFile(new File(directory, "graph.nodes.cypher")));
    }

    @Test public void testExportGraphCypherRelationships() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(cypherOutput.relationships(), readFile(new File(directory, "graph.relationships.cypher")));
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(cypherOutput.schema(), readFile(new File(directory, "graph.schema.cypher")));
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(cypherOutput.cleanup(), readFile(new File(directory, "graph.cleanup.cypher")));
    }

    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(4L, r.get("properties"));
        assertEquals(output.getAbsolutePath(), r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertEquals(true, ((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportQueryCypherNullFormat() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "plain")), (r) -> {
        });
        assertEquals(plainOutput.all(), readFile(output));
    }
}
