import apoc.ApocSignatures;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import apoc.util.TestContainerUtil.Neo4jVersion;
import apoc.util.TestContainerUtil.ApocPackage;
import org.junit.Test;

import org.neo4j.driver.Session;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static apoc.util.TestContainerUtil.createDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 This test is just to verify if the APOC procedures and functions are correctly deployed into a Neo4j instance without any startup issue.
 If you don't have docker installed it will fail, and you can simply ignore it.
 */
public class StartupTest {

    private static final File APOC_FULL;
    private static final File APOC_CORE;

    @Test
    public void check_basic_deployment() {
        for (var version: Neo4jVersion.values()) {
            try (Neo4jContainerExtension neo4jContainer = createDB(version, List.of(ApocPackage.CORE), !TestUtil.isRunningInCI())
                    .withNeo4jConfig("dbms.transaction.timeout", "60s")) {

                neo4jContainer.start();
                assertTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

                Session session = neo4jContainer.getSession();
                int procedureCount = session.run("SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc' RETURN count(*) AS count").peek().get("count").asInt();
                int functionCount = session.run("SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc' RETURN count(*) AS count").peek().get("count").asInt();
                int coreCount = session.run("CALL apoc.help('') YIELD core WHERE core = true RETURN count(*) AS count").peek().get("count").asInt();

                assertTrue(procedureCount > 0);
                assertTrue(functionCount > 0);
                assertTrue(coreCount > 0);
            } catch (Exception ex) {
                // if Testcontainers wasn't able to retrieve the docker image we ignore the test
                if (TestContainerUtil.isDockerImageAvailable(ex)) {
                    ex.printStackTrace();
                    fail("Should not have thrown exception when trying to start Neo4j: " + ex);
                } else if (!TestUtil.isRunningInCI()) {
                    fail( "The docker image " + TestContainerUtil.neo4jEnterpriseDockerImageVersion + " should be available in the CI. Exception:" + ex);
                }
            }
        }
    }

    @Test
    public void compare_with_sources() {
        for (var version: Neo4jVersion.values()) {
            try (Neo4jContainerExtension neo4jContainer = createDB(version, List.of(ApocPackage.CORE), !TestUtil.isRunningInCI())) {
                neo4jContainer.start();

                assertTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

                try (Session session = neo4jContainer.getSession()) {
                    final List<String> functionNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'function' RETURN name")
                            .list(record -> record.get("name").asString());
                    final List<String> procedureNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'procedure' RETURN name")
                            .list(record -> record.get("name").asString());


                    assertEquals(sorted(ApocSignatures.PROCEDURES), procedureNames);
                    assertEquals(sorted(ApocSignatures.FUNCTIONS), functionNames);
                }
            } catch (Exception ex) {
                if (TestContainerUtil.isDockerImageAvailable(ex)) {
                    ex.printStackTrace();
                    fail("Should not have thrown exception when trying to start Neo4j: " + ex);
                } else {
                    fail( "The docker image " + TestContainerUtil.neo4jEnterpriseDockerImageVersion + " should be available in the CI. Exception:" + ex);
                }
            }
        }
    }

    @Test
    public void checkFullWithExtraDependenciesJars() throws IOException {

        // we retrieve every full procedure and function via the extended.txt file
        final File extendedFile = new File(APOC_FULL, "src/main/resources/extended.txt");
        final List<String> expectedFullProcAndFunNames = FileUtils.readLines(extendedFile, StandardCharsets.UTF_8);

        // we check that with apoc-full jar and all extra-dependencies jars every procedure/function is detected
        startNeo4jContainerSession(() -> createEnterpriseDB(APOC_FULL, !TestUtil.isRunningInCI(), true),
                session -> {
                    checkCoreProcsAndFuncsExistence(session);

                    // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
                    final List<String> actualFullProcAndFunNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = false RETURN name").list(i -> i.get("name").asString());
                    assertEquals(sorted(expectedFullProcAndFunNames), sorted(actualFullProcAndFunNames));
                });
    }

    @Test
    public void checkCoreWithExtraDependenciesJars() {
        // we check that with apoc-core jar and all extra-dependencies jars every procedure/function is detected
        startNeo4jContainerSession(() -> createEnterpriseDB(APOC_CORE, !TestUtil.isRunningInCI(), true),
                this::checkCoreProcsAndFuncsExistence);

    }

    private void startNeo4jContainerSession(Supplier<Neo4jContainerExtension> neo4jContainerCreation,
                                            Consumer<Session> sessionConsumer) {
        try (final Neo4jContainerExtension neo4jContainer = neo4jContainerCreation.get()) {
            neo4jContainer.start();
            assertTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

            final Session session = neo4jContainer.getSession();

            sessionConsumer.accept(session);
        } catch (Exception ex) {
            // if Testcontainers wasn't able to retrieve the docker image we ignore the test
            if (TestContainerUtil.isDockerImageAvailable(ex)) {
                ex.printStackTrace();
                fail("Should not have thrown exception when trying to start Neo4j: " + ex);
            } else {
                fail( "The docker image could not be loaded. Check whether it's available locally / in the CI. Exception:" + ex);
            }
        }
    }

    private void checkCoreProcsAndFuncsExistence(Session session) {
        final List<String> functionNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'function' RETURN name")
                .list(record -> record.get("name").asString());
        final List<String> procedureNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = true and type = 'procedure' RETURN name")
                .list(record -> record.get("name").asString());

        assertEquals(sorted(ApocSignatures.PROCEDURES), procedureNames);
        assertEquals(sorted(ApocSignatures.FUNCTIONS), functionNames);
    }

    private List<String> sorted(List<String> signatures) {
        return signatures.stream().sorted().collect(Collectors.toList());
    }
}
