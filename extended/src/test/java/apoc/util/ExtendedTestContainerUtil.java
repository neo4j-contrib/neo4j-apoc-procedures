package apoc.util;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static apoc.util.ExtendedUtil.retryRunnable;
import static apoc.util.TestContainerUtil.copyFilesToPlugin;
import static apoc.util.TestContainerUtil.executeGradleTasks;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class ExtendedTestContainerUtil
{
    public static TestcontainersCausalCluster createEnterpriseCluster( List<TestContainerUtil.ApocPackage> apocPackages, int numOfCoreInstances, int numberOfReadReplica, Map<String, Object> neo4jConfig, Map<String, String> envSettings) {
        return TestcontainersCausalCluster.create(apocPackages, numOfCoreInstances, numberOfReadReplica, Duration.ofMinutes(4), neo4jConfig, envSettings);
    }

    public static <T> T singleResultFirstColumn(Session session, String cypher) {
        return singleResultFirstColumn(session, cypher, Map.of());
    }
    
    public static <T> T singleResultFirstColumn(Session session, String cypher, Map<String, Object> params) {
        return (T) session.executeWrite(tx -> tx.run(cypher, params).single().fields().get(0).value().asObject());
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

            retryRunnable(10, () -> {
                String neo4jUrl = "neo4j://localhost:" + container.getMappedPort(7687);
                AuthToken authToken = AuthTokens.basic("neo4j", container.getAdminPassword());
                Driver driver = GraphDatabase.driver(neo4jUrl, authToken);
                
                Session session = driver.session(SessionConfig.forDatabase(SYSTEM_DATABASE_NAME));
                sessionConsumer.accept(session, container);

                session.close();
                driver.close();
            });
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
        return instance.getEnvMap().get("NEO4J_server_bolt_advertised__address");
    }

    public static boolean dbIsWriter(String dbName, Session session, String boltAddress) {
        return session.run( "SHOW DATABASE $dbName WHERE address = $boltAddress",
                        Map.of("dbName", dbName, "boltAddress", boltAddress) )
                .single().get("writer")
                .asBoolean();
    }

    /**
     * Creates a Consumer that binds container ports to the same fixed host ports.
     *
     * @param ports The container ports to be exposed and bound.
     * @return A Consumer<CreateContainerCmd> to be used with .withCreateContainerCmdModifier().
     */
    public static Consumer<CreateContainerCmd> createPortBindingModifier(int... ports) {
        return cmd -> {
            Ports portBindings = new Ports();
            for (int port : ports) {
                portBindings.bind(ExposedPort.tcp(port), Ports.Binding.bindPort(port));
            }
            HostConfig hostConfig = cmd.getHostConfig();
            if (hostConfig == null) {
                hostConfig = new HostConfig();
                cmd.withHostConfig(hostConfig);
            }
            hostConfig.withPortBindings(portBindings);
        };
    }

}
