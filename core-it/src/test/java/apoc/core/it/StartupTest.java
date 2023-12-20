package apoc.core.it;

import static apoc.util.TestContainerUtil.createDB;
import static apoc.util.TestContainerUtil.dockerImageForNeo4j;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

import apoc.ApocSignatures;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.TestContainerUtil.Neo4jVersion;
import apoc.util.TestUtil;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Test;
import org.neo4j.driver.Session;

/*
This test is just to verify if the APOC procedures and functions are correctly deployed into a Neo4j instance without any startup issue.
If you don't have docker installed it will fail, and you can simply ignore it.
*/
public class StartupTest {

    @Test
    public void check_basic_deployment() {
        for (var version : Neo4jVersion.values()) {
            try {
                Neo4jContainerExtension neo4jContainer = createDB(
                                version, List.of(ApocPackage.CORE), !TestUtil.isRunningInCI())
                        .withNeo4jConfig("dbms.transaction.timeout", "60s");

                neo4jContainer.start();

                Session session = neo4jContainer.getSession();
                int procedureCount = session.run(
                                "SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc' RETURN count(*) AS count")
                        .peek()
                        .get("count")
                        .asInt();
                int functionCount = session.run(
                                "SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc' RETURN count(*) AS count")
                        .peek()
                        .get("count")
                        .asInt();
                int coreCount = session.run("CALL apoc.help('') YIELD core WHERE core = true RETURN count(*) AS count")
                        .peek()
                        .get("count")
                        .asInt();
                String startupLog = neo4jContainer.getLogs();

                assertTrue(procedureCount > 0);
                assertTrue(functionCount > 0);
                assertTrue(coreCount > 0);
                // Check there's one and only one logger for apoc inside the container
                // and it doesn't override the one inside the database
                assertFalse(startupLog.contains("[main] INFO org.eclipse.jetty.server.Server"));
                assertFalse(startupLog.contains("SLF4J: No SLF4J providers were found"));
                assertFalse(startupLog.contains("SLF4J: Failed to load class \"org.slf4j.impl.StaticLoggerBinder\""));
                assertFalse(startupLog.contains("SLF4J: Class path contains multiple SLF4J providers"));

                session.close();
                neo4jContainer.close();
            } catch (Exception ex) {
                // if Testcontainers wasn't able to retrieve the docker image we ignore the test
                if (TestContainerUtil.isDockerImageAvailable(ex)) {
                    ex.printStackTrace();
                    fail("Should not have thrown exception when trying to start Neo4j: " + ex);
                } else if (!TestUtil.isRunningInCI()) {
                    fail("The docker image " + dockerImageForNeo4j(version)
                            + " could not be loaded. Check whether it's available locally / in the CI. Exception:"
                            + ex);
                }
            }
        }
    }

    @Test
    public void check_cypherInitializer_waits_for_systemDb_to_be_available() {
        // we check that with apoc-core jar and all extra-dependencies jars every procedure/function is detected
        var version = Neo4jVersion.ENTERPRISE;
        try {
            Neo4jContainerExtension neo4jContainer = createDB(
                            version, List.of(ApocPackage.CORE), !TestUtil.isRunningInCI())
                    .withEnv(
                            "apoc.initializer.system.0",
                            "CREATE USER dummy IF NOT EXISTS SET PASSWORD \"pass12345\" CHANGE NOT REQUIRED")
                    .withEnv("apoc.initializer.system.1", "GRANT ROLE reader TO dummy");
            neo4jContainer.start();

            try (Session session = neo4jContainer.getSession()) {
                assertEventually(
                        () -> session.run("SHOW USERS YIELD roles, user WHERE user = 'dummy' RETURN roles").stream()
                                .collect(Collectors.toList()),
                        (result) -> result.size() > 0
                                && result.get(0).get("roles").asList().contains("reader"),
                        30,
                        TimeUnit.SECONDS);
            }

            String logs = neo4jContainer.getLogs();
            assertTrue(
                    logs.contains(
                            "successfully initialized: CREATE USER dummy IF NOT EXISTS SET PASSWORD '******' CHANGE NOT REQUIRED"));
            assertTrue(logs.contains("successfully initialized: GRANT ROLE reader TO dummy"));
            // The password should have been redacted
            assertFalse(logs.contains("pass12345"));
            neo4jContainer.close();
        } catch (Exception ex) {
            if (TestContainerUtil.isDockerImageAvailable(ex)) {
                ex.printStackTrace();
                fail("Should not have thrown exception when trying to start Neo4j: " + ex);
            } else {
                fail("The docker image " + dockerImageForNeo4j(version)
                        + " could not be loaded. Check whether it's available locally / in the CI. Exception:" + ex);
            }
        }
    }

    @Test
    public void compare_with_sources() {
        for (var version : Neo4jVersion.values()) {
            try {
                Neo4jContainerExtension neo4jContainer =
                        createDB(version, List.of(ApocPackage.CORE), !TestUtil.isRunningInCI());
                neo4jContainer.start();

                try (Session session = neo4jContainer.getSession()) {
                    final List<String> functionNames = session.run(
                                    "CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'function' RETURN name")
                            .list(record -> record.get("name").asString());
                    final List<String> procedureNames = session.run(
                                    "CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'procedure' RETURN name")
                            .list(record -> record.get("name").asString());

                    assertEquals(sorted(ApocSignatures.PROCEDURES), procedureNames);
                    assertEquals(sorted(ApocSignatures.FUNCTIONS), functionNames);
                }

                neo4jContainer.close();
            } catch (Exception ex) {
                if (TestContainerUtil.isDockerImageAvailable(ex)) {
                    ex.printStackTrace();
                    fail("Should not have thrown exception when trying to start Neo4j: " + ex);
                } else {
                    fail("The docker image " + dockerImageForNeo4j(version)
                            + " could not be loaded. Check whether it's available locally / in the CI. Exception:"
                            + ex);
                }
            }
        }
    }

    private List<String> sorted(List<String> signatures) {
        return signatures.stream().sorted().collect(Collectors.toList());
    }
}
