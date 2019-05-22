package apoc.export.cypher;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.driver.v1.Session;

import java.io.File;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestContainerUtil.*;
import static apoc.util.TestUtil.isTravis;
import static apoc.util.TestUtil.readFileToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

/**
 * @author as
 * @since 13.02.19
 */
public class ExportCypherEnterpriseFeaturesTest {

    private static File directory = new File("target/import");

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    private static String PREFIX = "/";

    @BeforeClass
    public static void beforeAll() {
        TestUtil.ignoreException(() -> {
            neo4jContainer = createEnterpriseDB(true)
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
    @Ignore("Missing constants")
    public void testExportWithCompoundConstraintCypherShell() {
        String fileName = "testCypherShellWithCompoundConstraint.cypher";
        testCall(session, "CALL apoc.export.cypher.all({file}, {config})",
                map("file", getFilePath(fileName), "config", Util.map("format", "cypher-shell")), (r) -> {
                    // assertExportStatement(EXPECTED_CYPHER_SHELL_WITH_COMPOUND_CONSTRAINT, r, fileName);
                });
    }

    @Test
    @Ignore("Missing constants")
    public void testExportWithCompoundConstraintPlain() {
        String fileName = "testPlainFormatWithCompoundConstraint.cypher";
        testCall(session, "CALL apoc.export.cypher.all({file}, {config})",
                map("file", getFilePath(fileName), "config", Util.map("format", "plain")),
                (r) -> {
                    // assertExportStatement(EXPECTED_PLAIN_FORMAT_WITH_COMPOUND_CONSTRAINT, r, fileName)
                });
    }

    @Test
    @Ignore("Missing constants")
    public void testExportWithCompoundConstraintNeo4jShell() {
        String fileName = "testNeo4jShellWithCompoundConstraint.cypher";
        testCall(session, "CALL apoc.export.cypher.all({file},{config})",
                map("file", getFilePath(fileName), "config", Util.map("format", "neo4j-shell")),
                (r) -> {
                    // assertExportStatement(EXPECTED_NEO4J_SHELL_WITH_COMPOUND_CONSTRAINT, r, fileName)
                });
    }

    private void assertExportStatement(String expectedStatement, Map<String, Object> result, String fileName) {
        assertEquals(expectedStatement, isTravis() ? result.get("cypherStatements") : readFileToString(new File(directory, fileName)));
    }

    private String getFilePath(String fileName) { // TODO on Travis we get FileNotFoundException with "Permission Denied" so we test the Cypher from the map (investigate why)
        return isTravis() ? "" : PREFIX + getImportDirectoryPath() + File.separator + fileName;
    }

    private String getImportDirectoryPath() {
        return directory.getPath().replace("target" + File.separator, "");
    }
}