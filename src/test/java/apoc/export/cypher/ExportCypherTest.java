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
import java.util.*;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypherTest {

    private static final String EXPECTED_NEO4J_SHELL = createExpectedStatement(plainFragments("full"),"neo4j-shell","full");

    private static final String EXPECTED_NODES = String.format("begin%n" +
            "CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
            "CREATE (:`Bar` {`name`:\"bar\", `age`:42});%n" +
            "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`age`:12, `UNIQUE IMPORT ID`:2});%n" +
            "commit");

    private static final String EXPECTED_SCHEMA = String.format("begin%n" +
            "CREATE INDEX ON :`Foo`(`name`);%n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
            "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "commit%n" +
            "schema await");

    private static final String EXPECTED_RELATIONSHIPS = String.format("begin%n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) CREATE (n1)-[:`KNOWS`]->(n2);%n" +
            "commit");

    private static final String EXPECTED_CLEAN_UP = String.format("begin%n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
            "commit%n" +
            "begin%n" +
            "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "commit");

    private static final String EXPECTED_CYPHER_SHELL = createExpectedStatement(plainFragments("full"), "cypher-shell", "full");

    private static final String EXPECTED_QUERY_NEO4J_SHELL = createExpectedStatement(plainFragments("query"), "neo4j-shell", "query");

    private static final String EXPECTED_QUERY_PLAIN_FORMAT = createExpectedStatement(plainFragments("query"), "plain", "query");

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
        assertEquals(EXPECTED_NEO4J_SHELL, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test public void testExportAllCypherForCypherShell() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{config})",
                map("file", output.getAbsolutePath(), "config", Util.map("format", "cypher-shell")), (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_CYPHER_SHELL, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test public void testExportQueryCypherForNeo4j() throws Exception {
        File output = new File(directory, "all.cypher");
        String query = "MATCH (n) RETURN n LIMIT 1";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_QUERY_NEO4J_SHELL, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test public void testExportGraphCypher() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},null) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_NEO4J_SHELL, new Scanner(output).useDelimiter("\\Z").next());
    }

    // -- Separate files tests -- //
    @Test public void testExportAllCypherNodes() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_NODES, new Scanner(new File(directory, "all.nodes.cypher")).useDelimiter("\\Z").next());
    }

    @Test public void testExportAllCypherRelationships() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_RELATIONSHIPS, new Scanner(new File(directory, "all.relationships.cypher")).useDelimiter("\\Z").next());
    }

    @Test public void testExportAllCypherSchema() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_SCHEMA, new Scanner(new File(directory, "all.schema.cypher")).useDelimiter("\\Z").next());
    }

    @Test public void testExportAllCypherCleanUp() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED_CLEAN_UP, new Scanner(new File(directory, "all.cleanup.cypher")).useDelimiter("\\Z").next());
    }

    @Test public void testExportGraphCypherNodes() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig), (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_NODES, new Scanner(new File(directory, "graph.nodes.cypher")).useDelimiter("\\Z").next());
    }

    @Test public void testExportGraphCypherRelationships() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_RELATIONSHIPS, new Scanner(new File(directory, "graph.relationships.cypher")).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_SCHEMA, new Scanner(new File(directory, "graph.schema.cypher")).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        File output = new File(directory, "graph.cypher");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath(), "exportConfig", exportConfig),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_CLEAN_UP, new Scanner(new File(directory, "graph.cleanup.cypher")).useDelimiter("\\Z").next());
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
        String query = "MATCH (n) RETURN n LIMIT 1";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", output.getAbsolutePath(), "query", query, "config", Util.map("format", "plain")), (r) -> {
        });
        assertEquals(EXPECTED_QUERY_PLAIN_FORMAT, new Scanner(output).useDelimiter("\\Z").next());
    }

    private static String createExpectedStatement(List<String> fragment, String format, String returnType){
        String commit = format.equals("cypher-shell") ? String.format(":commit%n") : (format.equals("neo4j-shell") ? String.format("commit%n") : "");
        String begin = format.equals("cypher-shell") ? String.format(":begin%n") : (format.equals("neo4j-shell") ? String.format("begin%n") : "");
        String schemaAwait = format.equals("neo4j-shell") ? String.format("schema await%n") : "";
        String finalCommit = format.equals("cypher-shell") ? String.format("%n:commit") : (format.equals("neo4j-shell") ? String.format("%ncommit") : "");
        String statement = returnType.equals("query") ? (begin+fragment.get(0)+commit
                                +begin+fragment.get(1)+commit+schemaAwait
                                +begin+fragment.get(2)+commit
                                +begin+fragment.get(3)+finalCommit) :
                                (begin+fragment.get(0)+commit
                                +begin+fragment.get(1)+commit+schemaAwait
                                +begin+fragment.get(2)+commit
                                +begin+fragment.get(3)+commit
                                +begin+fragment.get(4)+finalCommit);

        return statement;
    }

    private static List<String> plainFragments(String returnType){
        List<String> fullFragment = new ArrayList<String>();
        fullFragment.add(String.format("CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
                "CREATE (:`Bar` {`name`:\"bar\", `age`:42});%n" +
                "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`age`:12, `UNIQUE IMPORT ID`:2});%n"));
        fullFragment.add(String.format("CREATE INDEX ON :`Foo`(`name`);%n" +
                "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n"));
        fullFragment.add(String.format("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) CREATE (n1)-[:`KNOWS`]->(n2);%n"));
        fullFragment.add(String.format("MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"));
        fullFragment.add(String.format("DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;"));

        List<String> queryFragment = new ArrayList<String>();
        queryFragment.add(String.format("CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `UNIQUE IMPORT ID`:0});%n"));
        queryFragment.add(String.format(
                "CREATE INDEX ON :`Foo`(`name`);%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n"));
        queryFragment.add(String.format("MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"));
        queryFragment.add(String.format("DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;"));

        return returnType.equals("query") ? queryFragment : fullFragment;
    }

}
