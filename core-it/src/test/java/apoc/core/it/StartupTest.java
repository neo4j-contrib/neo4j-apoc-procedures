package apoc.core.it;

import apoc.ApocSettings;
import apoc.ApocSignatures;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.config.Setting;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_MAX_DECOMPRESSION_RATIO;
import static apoc.ApocSettings.apoc_export_file_enabled;
import static apoc.ApocSettings.apoc_import_file_enabled;
import static apoc.ApocSettings.apoc_import_file_use__neo4j__config;
import static apoc.ApocSettings.apoc_jobs_pool_num_threads;
import static apoc.ApocSettings.apoc_max_decompression_ratio;
import static apoc.ApocSettings.apoc_ttl_enabled;
import static apoc.ApocSettings.apoc_ttl_schedule;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

/*
 This test is just to verify if the APOC procedures and functions are correctly deployed into a Neo4j instance without any startup issue.
 If you don't have docker installed it will fail, and you can simply ignore it.
 */
public class StartupTest {

    @Test
    public void check_basic_deployment() {
        startNeo4jContainerSession(() -> createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.FULL), !TestUtil.isRunningInCI())
                        .withNeo4jConfig("dbms.transaction.timeout", "5s"),
                session -> {
                    int procedureCount = session.run("CALL dbms.procedures() YIELD name WHERE name STARTS WITH 'apoc' RETURN count(*) AS count").peek().get("count").asInt();
                    int functionCount = session.run("CALL dbms.functions() YIELD name WHERE name STARTS WITH 'apoc' RETURN count(*) AS count").peek().get("count").asInt();
                    int coreCount = session.run("CALL apoc.help('') YIELD core WHERE core = true RETURN count(*) AS count").peek().get("count").asInt();

                    assertTrue(procedureCount > 0);
                    assertTrue(functionCount > 0);
                    assertTrue(coreCount > 0);
                }, neo4jContainer -> {}
        );
    }

    @Test
    public void compare_with_sources() {
        startNeo4jContainerSession(() -> createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.FULL), !TestUtil.isRunningInCI()),
                this::checkCoreProcsAndFuncsExistence,
                neo4jContainer -> {}
        );

    }

    @Test
    public void checkFullWithExtraDependenciesJars() throws IOException {

        // we retrieve every full procedure and function via the extended.txt file
        final File extendedFile = new File(TestContainerUtil.fullDir, "src/main/resources/extended.txt");
        final List<String> expectedFullProcAndFunNames = FileUtils.readLines(extendedFile, StandardCharsets.UTF_8);
        
        // we check that with apoc-full jar and all extra-dependencies jars every procedure/function is detected
        startNeo4jContainerSession(() -> createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.FULL), !TestUtil.isRunningInCI(), true, false, null),
                session -> {
                    checkCoreProcsAndFuncsExistence(session);

                    // all full procedures and functions are present, also the ones which require extra-deps, e.g. the apoc.export.xls.*
                    final List<String> actualFullProcAndFunNames = session.run("CALL apoc.help('') YIELD core, type, name WHERE core = false RETURN name").list(i -> i.get("name").asString());
                    assertEquals(sorted(expectedFullProcAndFunNames), sorted(actualFullProcAndFunNames));
                },
                neo4jContainer -> {}
        );
    }

    @Test
    public void checkCoreWithExtraDependenciesJars() {
        // we check that with apoc-core jar and all extra-dependencies jars every procedure/function is detected
        startNeo4jContainerSession(() -> createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), !TestUtil.isRunningInCI(), true, false, null),
                this::checkCoreProcsAndFuncsExistence, (neo4jContainer) -> {});

    }

    @Test
    public void checkCypherInitializerWaitsForSystemDbToBeAvailable() {
        // we check that with apoc-core jar and all extra-dependencies jars every procedure/function is detected
        startNeo4jContainerSession(() -> createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), !TestUtil.isRunningInCI(), true, false, null)
                .withEnv("apoc.initializer.system.0", "CREATE USER dummy IF NOT EXISTS SET PASSWORD \"pass12345\" CHANGE NOT REQUIRED")
                .withEnv("apoc.initializer.system.1", "GRANT ROLE reader TO dummy"),
                session -> {
                    assertEventually(() ->
                         session.run( "SHOW USERS YIELD roles, user WHERE user = 'dummy' RETURN roles" ).stream()
                                             .collect( Collectors.toList() ), (result) ->
                         result.size() > 0 && result.get(0).get( "roles" ).asList().contains("reader" ),
                                     30, TimeUnit.SECONDS
                    );


                }, neo4jContainer -> {
                    String logs = neo4jContainer.getLogs();
                    assertTrue(logs.contains("successfully initialized: CREATE USER dummy IF NOT EXISTS SET PASSWORD '******' CHANGE NOT REQUIRED"));
                    assertTrue(logs.contains("successfully initialized: GRANT ROLE reader TO dummy"));
                    // The password should have been redacted
                    assertFalse(logs.contains("pass12345"));
                }
        );
    }

    @Test
    public void checkApocConfigShowsInNeo4jConfig() {
        // we check that the apoc config is shown inside dbms.listConfig()
        startNeo4jContainerSession(() -> createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), !TestUtil.isRunningInCI(), false, false, null),
           session -> {
                var result = session.run( "CALL dbms.listConfig() YIELD name WHERE name STARTS WITH \"apoc\" RETURN name" ).stream().collect(Collectors.toList());
                assertConfigContains(result, apoc_max_decompression_ratio);
                assertConfigContains(result, apoc_import_file_enabled);
                assertConfigContains(result, apoc_export_file_enabled);
                assertConfigContains(result, apoc_ttl_enabled);
                assertConfigContains(result, apoc_ttl_schedule);
           }, neo4jContainer -> {}
        );
    }

    private <T>void assertConfigContains(List<Record> configList, Setting<T> setting) {
        assertTrue(configList.stream().anyMatch(r -> r.get("name").asString().contains(setting.name())));
    }
    
    private void startNeo4jContainerSession(Supplier<Neo4jContainerExtension> neo4jContainerCreation,
                                            Consumer<Session> sessionConsumer,
                                            Consumer<Neo4jContainerExtension> containerConsumer) {
        try (final Neo4jContainerExtension neo4jContainer = neo4jContainerCreation.get()) {
            neo4jContainer.start();
            assertTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());
            
            final Session session = neo4jContainer.getSession();

            sessionConsumer.accept(session);
            containerConsumer.accept(neo4jContainer);
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
