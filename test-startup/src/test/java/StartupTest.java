import apoc.ApocSignatures;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.TestContainerUtil.Neo4jVersion;
import apoc.util.TestContainerUtil.ApocPackage;
import org.junit.Test;

import org.neo4j.driver.Session;

import java.util.List;
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

    private List<String> sorted(List<String> signatures) {
        return signatures.stream().sorted().collect(Collectors.toList());
    }
}
