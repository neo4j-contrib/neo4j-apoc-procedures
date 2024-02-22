package apoc.util;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static apoc.util.TestContainerUtil.copyFilesToPlugin;
import static apoc.util.TestContainerUtil.executeGradleTasks;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class ExtendedTestContainerUtil
{
    public static TestcontainersCausalCluster createEnterpriseCluster( List<TestContainerUtil.ApocPackage> apocPackages, int numOfCoreInstances, int numberOfReadReplica, Map<String, Object> neo4jConfig, Map<String, String> envSettings) {
        return TestcontainersCausalCluster.create(apocPackages, numOfCoreInstances, numberOfReadReplica, Duration.ofMinutes(4), neo4jConfig, envSettings);
    }

    public static <T> T singleResultFirstColumn(Session session, String cypher) {
        return (T) session.executeWrite(tx -> tx.run(cypher).single().fields().get(0).value().asObject());
    }

    public static void testCallInReadTransaction(Session session, String call, Consumer<Map<String, Object>> consumer) {
        TestContainerUtil.testCallInReadTransaction(session, call, null, consumer);
    }

    public static void addExtraDependencies() {
        File extraDepsDir = new File(TestContainerUtil.baseDir, "extra-dependencies");
        // build the extra-dependencies
        executeGradleTasks(extraDepsDir, "buildDependencies");

        // add all extra deps to the plugin docker folder
        final File directory = new File(extraDepsDir, "build/allJars");
        final IOFileFilter instance = new WildcardFileFilter("*.jar");
        copyFilesToPlugin(directory, instance, TestContainerUtil.pluginsFolder);
    }

    /**
     * Open a `neo4j://` routing session for each cluster member against system db
     */
    public static void routingSessionForEachMembers(List<Neo4jContainerExtension> members,
                                           BiConsumer<Session, Neo4jContainerExtension> sessionConsumer) {

        for (Neo4jContainerExtension container: members) {
            // Bolt (routing) url
            String neo4jUrl = "neo4j://localhost:" + container.getMappedPort(7687);

            AuthToken authToken = AuthTokens.basic("neo4j", container.getAdminPassword());
            try (Driver driver = GraphDatabase.driver(neo4jUrl, authToken);
                 Session session = driver.session(SessionConfig.forDatabase(SYSTEM_DATABASE_NAME))) {
                sessionConsumer.accept(session, container);
            }
        }
    }

    public static Driver getDriverIfNotReplica(Neo4jContainerExtension container) {
        final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
        final Driver driver = container.getDriver();
        if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || driver == null) {
            return null;
        }
        return driver;
    }

    public static String getBoltAddress(Neo4jContainerExtension instance) {
        return instance.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
    }

    public static boolean dbIsWriter(String dbName, Session session, String boltAddress) {
        return session.run( "SHOW DATABASE $dbName WHERE address = $boltAddress",
                        Map.of("dbName", dbName, "boltAddress", boltAddress) )
                .single().get("writer")
                .asBoolean();
    }

}
