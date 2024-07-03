package apoc.neo4j.docker;

import apoc.ApocSignatures;
import apoc.util.ExtendedTestContainerUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestContainerUtil.Neo4jVersion;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static apoc.util.TestContainerUtil.ApocPackage.CORE;
import static apoc.util.TestContainerUtil.ApocPackage.EXTENDED;
import static apoc.util.TestContainerUtil.createDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 This test is just to verify if the APOC procedures and functions are correctly deployed into a Neo4j instance without any startup issue.
 */
public class StartupExtendedTest {
    private static final String APOC_HELP_QUERY = "CALL apoc.help('') YIELD core, type, name WHERE core = $core and type = $type RETURN name";
    private static final List<String> EXPECTED_EXTENDED_NAMES;

    static {
        // retrieve every extended procedure and function via the extended.txt file
        final File extendedFile = new File(TestContainerUtil.extendedDir, "src/main/resources/extended.txt");
        try {
            EXPECTED_EXTENDED_NAMES = FileUtils.readLines(extendedFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void checkCoreAndExtendedWithExtraDependenciesJars() {
        // we check that with apoc-extended, apoc-core jar and all extra-dependencies jars every procedure/function is detected
        startContainerSessionWithExtraDeps((version) -> createDB(version, List.of(CORE, EXTENDED), true),
                session -> {
                    checkCoreProcsAndFuncsExistence(session);

                    // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
                    final List<String> actualExtNames = getNames(session, APOC_HELP_QUERY,
                            Map.of("core", false, "type", "function") );
                    final List<String> functionExtNames = getNames(session, APOC_HELP_QUERY,
                            Map.of("core", false, "type", "procedure") );

                    actualExtNames.addAll(functionExtNames);

                    assertEquals(sorted(EXPECTED_EXTENDED_NAMES), sorted(actualExtNames));
                });
    }

    @Test
    public void checkExtendedWithExtraDependenciesJars() {
        // we check that with apoc-extended jar and all extra-dependencies jars every procedure/function is detected
        startContainerSessionWithExtraDeps((version) -> createDB(version, List.of(EXTENDED), true),
                session -> {
                    // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
                    final List<String> actualExtNames = getNames(session, "SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc.' RETURN name");
                    final List<String> functionExtNames = getNames(session, "SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc.' RETURN name");

                    actualExtNames.addAll(functionExtNames);

                    assertEquals(sorted(EXPECTED_EXTENDED_NAMES), sorted(actualExtNames));
                });
    }

    @Test
    public void checkCoreWithExtraDependenciesJars() {
        // we check that with apoc-core jar and all extra-dependencies jars every procedure/function is detected
        startContainerSessionWithExtraDeps((version) -> createDB(version, List.of(CORE), true),
                this::checkCoreProcsAndFuncsExistence);
    }

    private void startContainerSessionWithExtraDeps(Function<Neo4jVersion, Neo4jContainerExtension> neo4jContainerCreation,
                                                    Consumer<Session> sessionConsumer) {
        for (var version: Neo4jVersion.values()) {

            try (final Neo4jContainerExtension neo4jContainer = neo4jContainerCreation.apply(version)) {
                // add extra-deps before starting it
                ExtendedTestContainerUtil.addExtraDependencies();
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
                    fail("The docker image could not be loaded. Check whether it's available locally / in the CI. Exception:" + ex);
                }
            }
        }
    }

    private void checkCoreProcsAndFuncsExistence(Session session) {
        final List<String> functionNames = getNames(session, APOC_HELP_QUERY,
                Map.of("core", true, "type", "function") );

        final List<String> procedureNames = getNames(session, APOC_HELP_QUERY,
                Map.of("core", true, "type", "procedure") );

        assertEquals(sorted(ApocSignatures.PROCEDURES), procedureNames);
        assertEquals(sorted(ApocSignatures.FUNCTIONS), functionNames);
    }

    private static List<String> getNames(Session session, String query, Map<String, Object> params) {
        return session.run(query, params)
                .list(i -> i.get("name").asString());
    }

    private static List<String> getNames(Session session, String query) {
        return getNames(session, query, Collections.emptyMap());
    }

    private List<String> sorted(List<String> signatures) {
        return signatures.stream()
                .sorted()
                .collect(Collectors.toList());
    }
}