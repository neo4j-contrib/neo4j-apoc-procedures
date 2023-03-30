package apoc.cypher;

import apoc.util.Neo4jContainerExtension;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CypherInitializerEnterpriseFeaturesTest {
    private static final String PASSWORD = "pwd12345";
    private static final String DEFAULT_USER = "neo4j";
    private static final String OTHER_USER = "dummy";
    private static final String ROLE = "reader";

    private static String boltUrl;

    @BeforeClass
    public static void beforeAll() {
        String alterNeo4j = String.format("ALTER USER %s IF EXISTS SET PASSWORD '%s' CHANGE NOT REQUIRED", DEFAULT_USER, PASSWORD);
        String createOtherUser = String.format("CREATE USER %s IF NOT EXISTS SET PASSWORD '%s' CHANGE NOT REQUIRED", OTHER_USER, PASSWORD);
        String grantUserRole = String.format("GRANT ROLE %s TO %s", ROLE, OTHER_USER);
        Map<String, String> envMap = Map.of(
                "apoc.initializer.system.0", alterNeo4j,
                "apoc.initializer.system.1", createOtherUser,
                "apoc.initializer.system.2", grantUserRole
        );

        Neo4jContainerExtension neo4jContainer = createEnterpriseDB(true)
                .withEnv(envMap);
        neo4jContainer.start();
        boltUrl = neo4jContainer.getBoltUrl();

        assertTrue(neo4jContainer.isRunning());
    }

    @Test
    public void testInitRbacCommands() {
        // there may be a small delay to start the commands, because of CypherInitializer.awaitUntil()
        assertEventually(() -> {
            // check that `dummy` user exists
            try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(OTHER_USER, PASSWORD));
                 Session session = driver.session()) {
                // just to check that the user is correctly connected
                session.run("RETURN 1");
                return true;
            } catch (Exception e) {
                return false;
            }
        }, (value) -> value, 5L, TimeUnit.SECONDS);

        // check that the `neo4j` pwd is changed
        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(DEFAULT_USER, PASSWORD));
             Session session1 = driver.session()) {
            // check that `dummy` user has `reader` role
            testCall(session1, "SHOW USERS YIELD user, roles WHERE user = $user",
                    Map.of("user", OTHER_USER),
                    row -> {
                Set<String> expected = Set.of(ROLE, "PUBLIC");
                Set<String> actual = Set.copyOf((List<String>) row.get("roles"));
                assertEquals(expected, actual);
            });
        }
    }
}
