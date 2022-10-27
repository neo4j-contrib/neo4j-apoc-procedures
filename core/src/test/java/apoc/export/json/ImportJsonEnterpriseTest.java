package apoc.export.json;

import apoc.util.CompressionAlgo;
import apoc.util.Neo4jContainerExtension;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.File;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.export.json.JsonImporter.MISSING_CONSTRAINT_ERROR_MSG;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.Util.map;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ImportJsonEnterpriseTest {

    private static final String MISSING_USER_CONSTRAINT = format(MISSING_CONSTRAINT_ERROR_MSG, "User", "neo4jImportId");
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(true)
                .withEnv(APOC_IMPORT_FILE_ENABLED, "true");
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @Before
    public void before() {
        session.run("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void shouldFailWithExistenceConstraintDueToMissingUniqueConstraint() {
        final byte[] file = fileToBinary(new File(ImportJsonTest.directory, "all.json"), CompressionAlgo.GZIP.name());

        session.run("CREATE CONSTRAINT userExistence ON (n:User) assert n.neo4jImportId IS NOT NULL");

        // when
        try {
            testCall(session, "CALL apoc.import.json($file, $config)",
                    map("file", file, "config", map(COMPRESSION, CompressionAlgo.GZIP.name())),
                    r -> fail("Should fail due to missing constraint"));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(MISSING_USER_CONSTRAINT));
        } finally {
            session.run("DROP CONSTRAINT userExistence");
        }
        
    }

    @Test
    public void shouldFailWithMultipleNodeKeyDueToMissingUniqueConstraint() {
        final byte[] file = fileToBinary(new File(ImportJsonTest.directory, "all.json"), CompressionAlgo.GZIP.name());

        session.run("CREATE CONSTRAINT userMultipleKeys ON (n:User) assert (n.neo4jImportId, n.anotherProp) IS NODE KEY");

        // when
        try {
            testCall(session, "CALL apoc.import.json($file, $config)",
                    map("file", file, "config", map(COMPRESSION, CompressionAlgo.GZIP.name())),
                    r -> fail("Should fail due to missing constraint"));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(MISSING_USER_CONSTRAINT));
        } finally {
            session.run("DROP CONSTRAINT userMultipleKeys");
        }
        
    }
    
    @Test
    public void shouldImportWithNodeKeyConstraint() {
        final byte[] file = fileToBinary(new File(ImportJsonTest.directory, "all.json"), CompressionAlgo.GZIP.name());

        session.run("CREATE CONSTRAINT userNodeKey ON (n:User) assert (n.neo4jImportId) IS NODE KEY");

        try {
            // when
            testCall(session, "CALL apoc.import.json($file, $config)",
                    map("file", file, "config", map(COMPRESSION, CompressionAlgo.GZIP.name())),
                    r -> ImportJsonTest.assertionsAllJsonProgressInfo(r, true));
        } finally {
            session.run("DROP CONSTRAINT userNodeKey");
        }
    }
}
