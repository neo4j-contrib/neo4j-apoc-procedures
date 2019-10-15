package apoc.export.cypher;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.v1.Session;

import java.io.File;
import java.util.Map;

import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.*;
import static apoc.util.MapUtil.map;
import static apoc.util.TestContainerUtil.*;
import static apoc.util.TestUtil.isTravis;
import static apoc.util.TestUtil.readFileToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

/**
 * @author as
 * @since 13.02.19
 */
public class ExportCypherEnterpriseFeaturesTest {

    private static File directory = new File("target/import"); // it's the directory bounded to the /import dir inside the Neo4jContainer

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            executeGradleTasks("shadow");
            neo4jContainer = createEnterpriseDB(!TestUtil.isTravis())
                    .withInitScript("init_neo4j_export_csv.cypher");
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null) {
            neo4jContainer.close();
        }
        cleanBuild();
    }

    @Test
    public void testExportWithCompoundConstraintCypherShell() {
        String fileName = "testCypherShellWithCompoundConstraint.cypher";
        testCall(session, "CALL apoc.export.cypher.all({file}, {config})",
                map("file", fileName, "config", Util.map("format", "cypher-shell")), (r) -> {
                    assertExportStatement(EXPECTED_CYPHER_SHELL_WITH_COMPOUND_CONSTRAINT, r, fileName);
                });
    }

    @Test
    public void testExportWithCompoundConstraintPlain() {
        String fileName = "testPlainFormatWithCompoundConstraint.cypher";
        testCall(session, "CALL apoc.export.cypher.all({file}, {config})",
                map("file", fileName, "config", Util.map("format", "plain")),
                (r) -> assertExportStatement(EXPECTED_PLAIN_FORMAT_WITH_COMPOUND_CONSTRAINT, r, fileName));
    }

    @Test
    public void testExportWithCompoundConstraintNeo4jShell() {
        String fileName = "testNeo4jShellWithCompoundConstraint.cypher";
        testCall(session, "CALL apoc.export.cypher.all({file},{config})",
                map("file", fileName, "config", Util.map("format", "neo4j-shell")),
                (r) -> assertExportStatement(EXPECTED_NEO4J_SHELL_WITH_COMPOUND_CONSTRAINT, r, fileName));
    }

    private void assertExportStatement(String expectedStatement, Map<String, Object> result, String fileName) {
        assertEquals(expectedStatement, isTravis() ? result.get("cypherStatements") : readFileToString(new File(directory, fileName)));
    }
}