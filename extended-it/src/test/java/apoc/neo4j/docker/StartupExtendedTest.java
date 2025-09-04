package apoc.neo4j.docker;

import apoc.ApocSignatures;
import apoc.util.ExtendedTestContainerUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestContainerUtil.Neo4jVersion;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static apoc.util.ExtendedTestContainerUtil.extendedDir;
import static apoc.util.TestContainerUtil.ApocPackage.CORE;
import static apoc.util.TestContainerUtil.ApocPackage.EXTENDED;
import static apoc.util.TestContainerUtil.createDB;
import static apoc.util.TestContainerUtil.dockerImageForNeo4j;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test is just to verify if the APOC procedures and functions are correctly deployed into a Neo4j instance without any startup issue.
 * <br/>
 * NOTE: if we have an error like `Caused by: org.gradle.internal.resolve.ModuleVersionNotFoundException: Could not find org.neo4j:neo4j:5.25.0.`,
 * there is probably inconsistency between the neo4j version of APOC Core and the Extended version,
 * so we need to set the environment variable `NEO4JVERSION=<correctVersion>`, e.g. `NEO4JVERSION=5.25.1`,
 * which is valued via {@link TestContainerUtil#executeGradleTasks(File, String...)} present in APOC Core and therefore not modifiable by APOC Extended
 */
@Ignore
public class StartupExtendedTest {
    private static final String APOC_HELP_QUERY = "CALL apoc.help('') YIELD core, type, name WHERE core = $core and type = $type RETURN name";
    private static final List<String> EXPECTED_EXTENDED_NAMES_CYPHER_5;
    private static final List<String> EXPECTED_EXTENDED_NAMES_CYPHER_25;

    static {
        // retrieve every extended procedure and function via the extendedCypher5.txt file
        final File extendedFileCypher5 = new File(extendedDir, "src/main/resources/extendedCypher5.txt");
        final File extendedFileCypher25 = new File(extendedDir, "src/main/resources/extendedCypher25.txt");
        try {
            EXPECTED_EXTENDED_NAMES_CYPHER_5 = FileUtils.readLines(extendedFileCypher5, StandardCharsets.UTF_8);
            EXPECTED_EXTENDED_NAMES_CYPHER_25 = FileUtils.readLines(extendedFileCypher25, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void checkCoreAndExtendedWithExtraDependenciesJars() {
        // we check that with apoc-extended, apoc-core jar and all extra-dependencies jars every procedure/function is detected
        startContainerSessionWithExtraDeps((version) -> createDB(version, List.of(CORE, EXTENDED), true),
                session -> {

                    // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
                    checkExtendedProcsAndFuncsExistence(session);
                    checkCoreProcsAndFuncsExistence(session);
                });
    }

    @Test
    public void checkExtendedWithExtraDependenciesJars() {
        // we check that with apoc-extended jar and all extra-dependencies jars every procedure/function is detected
        // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
        startContainerSessionWithExtraDeps((version) -> createDB(version, List.of(EXTENDED), true),
                session -> {
                    // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
                    final List<String> actualExtNamesCypher5 = getNames(session, "CYPHER 5 SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc.' RETURN name");
                    final List<String> functionExtNamesCypher5 = getNames(session, "CYPHER 5 SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc.' RETURN name");

                    List<String> procsAndFuncsCypher5 = new ArrayList<>();
                    procsAndFuncsCypher5.addAll(actualExtNamesCypher5);
                    procsAndFuncsCypher5.addAll(functionExtNamesCypher5);
                    
                    assertEquals(sorted(EXPECTED_EXTENDED_NAMES_CYPHER_5), sorted(procsAndFuncsCypher5));
                    
                    final List<String> actualExtNamesCypher25 = getNames(session, "CYPHER 25 SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc.' RETURN name");
                    final List<String> functionExtNamesCypher25 = getNames(session, "CYPHER 25 SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc.' RETURN name");

                    List<String> procsAndFuncsCypher25 = new ArrayList<>();
                    procsAndFuncsCypher25.addAll(actualExtNamesCypher25);
                    procsAndFuncsCypher25.addAll(functionExtNamesCypher25);
                    
                    assertEquals(sorted(EXPECTED_EXTENDED_NAMES_CYPHER_25), sorted(procsAndFuncsCypher25));
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

            try (final Neo4jContainerExtension neo4jContainer = neo4jContainerCreation.apply(version)
                    .withNeo4jConfig("internal.dbms.cypher.enable_experimental_versions", "true")
            ) {
                // add extra-deps before starting it
                ExtendedTestContainerUtil.addExtraDependencies();
                neo4jContainer.start();
                assertTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

                String startupLog = neo4jContainer.getLogs();

                assertFalse(startupLog.contains("SLF4J: Failed to load class \"org.slf4j.impl.StaticLoggerBinder\""));
                assertFalse(startupLog.contains("SLF4J: Class path contains multiple SLF4J providers"));

                final Session session = neo4jContainer.getSession();

                sessionConsumer.accept(session);
            } catch (Exception ex) {
                // if Testcontainers wasn't able to retrieve the docker image we ignore the test
                if (TestContainerUtil.isDockerImageAvailable(ex)) {
                    ex.printStackTrace();
                    fail("Should not have thrown exception when trying to start Neo4j: " + ex);
                } else if (!TestUtil.isRunningInCI()) {
                    fail( "The docker image " + dockerImageForNeo4j(version) + " could not be loaded. Check whether it's available locally / in the CI. Exception:" + ex);
                } else {
                    fail( "The docker image " + dockerImageForNeo4j(version) + " could not be loaded. Check whether it's available locally / in the CI. Exception:" + ex);
                }
            }
        }
    }

    private void checkCoreProcsAndFuncsExistence(Session session) {
        final List<String> functionCypher5Names = getNames(session, "CYPHER 5 " + APOC_HELP_QUERY,
                Map.of("core", true, "type", "function") );

        final List<String> procedureCypher5Names = getNames(session, "CYPHER 5 " + APOC_HELP_QUERY,
                Map.of("core", true, "type", "procedure") );

        assertEquals(sorted(ApocSignatures.PROCEDURES_CYPHER_5), procedureCypher5Names);
        assertEquals(sorted(ApocSignatures.FUNCTIONS_CYPHER_5), functionCypher5Names);
        
        final List<String> functionCypher25Names = getNames(session, "CYPHER 25 " + APOC_HELP_QUERY,
                Map.of("core", true, "type", "function") );

        final List<String> procedureCypher25Names = getNames(session, "CYPHER 25 " + APOC_HELP_QUERY,
                Map.of("core", true, "type", "procedure") );

        assertEquals(sorted(ApocSignatures.PROCEDURES_CYPHER_25), procedureCypher25Names);
        assertEquals(sorted(ApocSignatures.FUNCTIONS_CYPHER_25), functionCypher25Names);
    }

    private void checkExtendedProcsAndFuncsExistence(Session session) {
        // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
        final List<String> actualExtNamesCypher5 = getNames(session, "CYPHER 5 " + APOC_HELP_QUERY,
                Map.of("core", false, "type", "function") );
        final List<String> functionExtNamesCypher5 = getNames(session, "CYPHER 5 " + APOC_HELP_QUERY,
                Map.of("core", false, "type", "procedure") );

        final List<String> actualExtNamesCypher25 = getNames(session, "CYPHER 25 " + APOC_HELP_QUERY,
                Map.of("core", false, "type", "function") );
        final List<String> functionExtNamesCypher25 = getNames(session, "CYPHER 25 " + APOC_HELP_QUERY,
                Map.of("core", false, "type", "procedure") );

        List<String> procsAndFuncsCypher5 = new ArrayList<>();
        procsAndFuncsCypher5.addAll(actualExtNamesCypher5);
        procsAndFuncsCypher5.addAll(functionExtNamesCypher5);

        List<String> procsAndFuncsCypher25 = new ArrayList<>();
        procsAndFuncsCypher25.addAll(actualExtNamesCypher25);
        procsAndFuncsCypher25.addAll(functionExtNamesCypher25);

        assertEquals(sorted(EXPECTED_EXTENDED_NAMES_CYPHER_5), sorted(procsAndFuncsCypher5));
        assertEquals(sorted(EXPECTED_EXTENDED_NAMES_CYPHER_25), sorted(procsAndFuncsCypher25));
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