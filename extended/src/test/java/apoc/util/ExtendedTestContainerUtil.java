package apoc.util;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

public class ExtendedTestContainerUtil
{
    public static TestcontainersCausalCluster createEnterpriseCluster( List<TestContainerUtil.ApocPackage> apocPackages, int numOfCoreInstances, int numberOfReadReplica, Map<String, Object> neo4jConfig, Map<String, String> envSettings) {
        return TestcontainersCausalCluster.create(apocPackages, numOfCoreInstances, numberOfReadReplica, Duration.ofMinutes(4), neo4jConfig, envSettings);
    }

    public static <T> T singleResultFirstColumn(Session session, String cypher) {
        return (T) session.writeTransaction(tx -> tx.run(cypher).single().fields().get(0).value().asObject());
    }

    public static void testCallInReadTransaction(Session session, String call, Consumer<Map<String, Object>> consumer) {
        TestContainerUtil.testCallInReadTransaction(session, call, null, consumer);
    }

    /**
     * Open a routing session for each cluster core member
     *
     * @param members
     * @param sessionConsumer
     */
    public static void queryForEachMembers(List<Neo4jContainerExtension> members,
                                           BiConsumer<Session, Neo4jContainerExtension> sessionConsumer) {

        for (Neo4jContainerExtension container: members) {
            // Bolt (routing) url
            String neo4jUrl = "neo4j://localhost:" + container.getMappedPort(7687);
            AuthToken auth = AuthTokens.basic("neo4j", container.getAdminPassword());

            try (Driver driver = GraphDatabase.driver(neo4jUrl, auth);
                 Session session = driver.session()) {
                sessionConsumer.accept(session, container);
            }
        }
    }
}
