package apoc.export.json;

import apoc.util.Neo4jContainerExtension;
import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.util.List;
import java.util.Map;

import static apoc.export.json.JsonImporter.MISSING_CONSTRAINT_ERROR_MSG;
import static apoc.util.MapUtil.map;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testResult;
import static java.lang.String.format;

public class ImportJsonEnterpriseFeaturesTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(true);
        neo4jContainer.start();
        session = neo4jContainer.getSession();

        session.writeTransaction(tx -> {
            tx.run("CREATE (u:User {name: \"Fred\"})");
            tx.commit();
            return null;
        });

        // First export a file that we can import
        String filename = "all.json";
        testResult(session, "CALL apoc.export.json.all($file,null)",
                map("file", filename),
                (r) -> Assert.assertTrue(r.hasNext())
        );

        // Remove node so we can import it after
        session.writeTransaction(tx -> {
            tx.run("MATCH (n) DETACH DELETE n");
            tx.commit();
            return null;
        });
    }

    @Before
    public void cleanUpDb() {
        // Remove all current constraints/indexes
        session.writeTransaction(tx -> {
            final List<String> constraints = tx.run("SHOW CONSTRAINTS YIELD name").list(i -> i.get("name").asString());
            constraints.forEach(name -> tx.run(String.format("DROP CONSTRAINT %s", name)));

            final List<String> indexes = tx.run("SHOW INDEXES YIELD name").list(i -> i.get("name").asString());
            indexes.forEach(name -> tx.run(String.format("DROP INDEX %s", name)));
            tx.commit();
            return null;
        });

        // Make sure we have no other nodes so all uniqueness constraints work
        session.writeTransaction(tx -> {
            tx.run("MATCH (n) DETACH DELETE n");
            tx.commit();
            return null;
        });
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void shouldFailBecauseOfMissingUniqueConstraintException() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.neo4jImportId IS NOT NULL");
            tx.run("CREATE CONSTRAINT FOR (n:User) REQUIRE (n.neo4jImportId, n.name) IS NODE KEY");
            tx.commit();
            return null;
        });

        String filename = "all.json";
        Exception e = Assert.assertThrows(Exception.class,
                () ->  testResult(session, "CALL apoc.import.json($file, {})", map("file", filename), (result) -> {})
        );

        String expectedMsg = format(MISSING_CONSTRAINT_ERROR_MSG, "User", "neo4jImportId");
        TestCase.assertTrue(e.getMessage().contains(expectedMsg));
    }

    @Test
    public void shouldWorkWithSingularNodeKeyAsConstraint() {
        // Add unique constraint (test if single node key works)
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR (n:User) REQUIRE (n.neo4jImportId) IS NODE KEY");
            tx.commit();
            return null;
        });

        // Test import procedure
        String filename = "all.json";
        testResult(session, "CALL apoc.import.json($file, {})",
                map("file", filename),
                (result) -> {
                    Map<String, Object> r = result.next();
                    Assert.assertEquals("all.json", r.get("file"));
                    Assert.assertEquals("file", r.get("source"));
                    Assert.assertEquals("json", r.get("format"));
                    Assert.assertEquals(1L, r.get("nodes"));
                    Assert.assertEquals(0L, r.get("relationships"));
                    Assert.assertEquals(2L, r.get("properties"));
                    Assert.assertEquals(1L, r.get("rows"));
                    Assert.assertEquals(true, r.get("done"));
                }
        );
    }
}
