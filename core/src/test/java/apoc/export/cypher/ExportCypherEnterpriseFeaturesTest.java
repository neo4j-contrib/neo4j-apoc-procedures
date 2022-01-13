package apoc.export.cypher;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.File;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.*;
import static apoc.util.MapUtil.map;
import static apoc.util.TestContainerUtil.*;
import static apoc.util.TestUtil.isRunningInCI;
import static apoc.util.TestUtil.readFileToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

/**
 * @author as
 * @since 13.02.19
 */
public class ExportCypherEnterpriseFeaturesTest {

    private static File directory = new File("import"); // it's the directory bounded to the /import dir inside the Neo4jContainer

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            // We build the project, the artifact will be placed into ./build/libs
            neo4jContainer = createEnterpriseDB(!TestUtil.isRunningInCI())
                    .withInitScript("init_neo4j_export_csv.cypher");
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null && neo4jContainer.isRunning()) {
            neo4jContainer.close();
        }
    }

    private static void beforeTwoLabelsWithOneCompoundConstraintEach() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON (t:Base) ASSERT (t.tenantId, t.id) IS NODE KEY");
            tx.commit();
            return null;
        });
        session.writeTransaction(tx -> {
            tx.run("CREATE (a:Person:Base {name: 'Phil', surname: 'Meyer', tenantId: 'neo4j', id: 'waBfk3z'}) " +
                    "CREATE (b:Person:Base {name: 'Silvia', surname: 'Jones', tenantId: 'random', id: 'waBfk3z'}) " +
                    "CREATE (a)-[:KNOWS]->(b)");
            tx.commit();
            return null;
        });
    }

    private static void afterTwoLabelsWithOneCompoundConstraintEach() {
        session.writeTransaction(tx -> {
            tx.run("MATCH (a:Person:Base) DETACH DELETE a");
            tx.commit();
            return null;
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT ON (t:Base) ASSERT (t.tenantId, t.id) IS NODE KEY");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testExportWithCompoundConstraintCypherShell() {
        String fileName = "testCypherShellWithCompoundConstraint.cypher";
        testCall(session, "CALL apoc.export.cypher.all($file, $config)",
                map("file", fileName, "config", Util.map("format", "cypher-shell")), (r) -> {
                    assertExportStatement(EXPECTED_CYPHER_SHELL_WITH_COMPOUND_CONSTRAINT, r, fileName);
                });
    }

    @Test
    public void testExportWithCompoundConstraintPlain() {
        String fileName = "testPlainFormatWithCompoundConstraint.cypher";
        testCall(session, "CALL apoc.export.cypher.all($file, $config)",
                map("file", fileName, "config", Util.map("format", "plain")),
                (r) -> assertExportStatement(EXPECTED_PLAIN_FORMAT_WITH_COMPOUND_CONSTRAINT, r, fileName));
    }

    @Test
    public void testExportWithCompoundConstraintNeo4jShell() {
        String fileName = "testNeo4jShellWithCompoundConstraint.cypher";
        testCall(session, "CALL apoc.export.cypher.all($file, $config)",
                map("file", fileName, "config", Util.map("format", "neo4j-shell")),
                (r) -> assertExportStatement(EXPECTED_NEO4J_SHELL_WITH_COMPOUND_CONSTRAINT, r, fileName));
    }

    @Test
    public void shouldHandleTwoLabelsWithOneCompoundConstraintEach() {
        final String query = "MATCH (a:Person:Base)-[r:KNOWS]-(b:Person) RETURN a, b, r";
        /* The bug was:
            UNWIND [{start: {name:"Phil", surname:"Meyer"}, end: {name:"Silvia", surname:"Jones"}, properties:{}}] AS row
            MATCH (start:Person{tenantId: row.start.tenantId, id: row.start.id, surname: row.start.surname, name: row.start.name})
            MATCH (end:Person{surname: row.end.surname, name: row.end.name})
            CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;
         */
        final String expected = "UNWIND [{start: {name:\"Phil\", surname:\"Meyer\"}, " +
                "end: {name:\"Silvia\", surname:\"Jones\"}, properties:{}}] AS row\n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})\n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})\n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties";

        try {
            beforeTwoLabelsWithOneCompoundConstraintEach();
            testCallInReadTransaction(session, "CALL apoc.export.cypher.query($query, $file, $config)",
                    Util.map("file", null, "query", query, "config", Util.map("format", "plain", "stream", true)), (r) -> {
                        final String cypherStatements = (String) r.get("cypherStatements");
                        String unwind = Stream.of(cypherStatements.split(";"))
                                .map(String::trim)
                                .filter(s -> s.startsWith("UNWIND"))
                                .filter(s -> s.contains("Meyer"))
                                .skip(1)
                                .findFirst()
                                .orElse(null);
                        assertEquals(expected, unwind);
                    });
        } finally {
            afterTwoLabelsWithOneCompoundConstraintEach();
        }
    }

    private void assertExportStatement(String expectedStatement, Map<String, Object> result, String fileName) {
        assertEquals(expectedStatement, isRunningInCI() ? result.get("cypherStatements") : readFileToString(new File(directory, fileName)));
    }
}
