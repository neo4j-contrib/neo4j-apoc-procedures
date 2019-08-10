package apoc.util;

import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.printFullStackTrace;
import static org.junit.Assert.*;

public class TestContainerUtil {

    private TestContainerUtil() {}

    private static File baseDir = Paths.get(".").toFile();




    public static TestcontainersCausalCluster createEnterpriseCluster(int numOfCoreInstances, int numberOfReadReplica, Map<String, Object> neo4jConfig) {
        return TestcontainersCausalCluster.create(numOfCoreInstances, numberOfReadReplica, Duration.ofMinutes(4), neo4jConfig);
    }

    public static Neo4jContainerExtension createEnterpriseDB(boolean withLogging) {
        // We define the container with external volumes
        Neo4jContainerExtension neo4jContainer = new Neo4jContainerExtension("neo4j:3.5.3-enterprise")
                .withPlugins(MountableFile.forHostPath("./target/tests/gradle-build/libs")) // map the apoc's artifact dir as the Neo4j's plugin dir
                .withAdminPassword("apoc")
                .withNeo4jConfig("apoc.export.file.enabled", "true")
                .withNeo4jConfig("dbms.security.procedures.unrestricted", "apoc.*")
//                .withEnv("NEO4J_wrapper_java_additional","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -Xdebug-Xnoagent-Djava.compiler=NONE-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005")
                .withFileSystemBind("./target/import", "/import") // map the "target/import" dir as the Neo4j's import dir
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes");
        if (withLogging) {
            neo4jContainer.withLogging();
        }
        return neo4jContainer;
    }

    public static void executeGradleTasks(String... tasks) {
        ProjectConnection connection = GradleConnector.newConnector()
                .forProjectDirectory(baseDir)
                .useBuildDistribution()
                .connect();
        try {
            connection.newBuild()
                    .withArguments("-Dorg.gradle.project.buildDir=./target/tests/gradle-build")
                    .forTasks(tasks)
                    .run();
        } finally {
            connection.close();
        }
    }

    public static void cleanBuild() {
        executeGradleTasks("clean");
    }

    public static void testCall(Session session, String call, Map<String,Object> params, Consumer<Map<String, Object>> consumer) {
        testResult(session, call, params, (res) -> {
            try {
                assertNotNull("result should be not null", res);
                assertTrue("result should be not empty", res.hasNext());
                Map<String, Object> row = res.next();
                consumer.accept(row);
                assertFalse("result should not have next", res.hasNext());
            } catch(Throwable t) {
                printFullStackTrace(t);
                throw t;
            }
        });
    }

    public static void testCall(Session session, String call, Consumer<Map<String, Object>> consumer) {
        testCall(session, call, null, consumer);
    }

    public static void testResult(Session session, String call, Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        testResult(session, call, null, resultConsumer);
    }

    public static void testResult(Session session, String call, Map<String,Object> params, Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        session.writeTransaction(tx -> {
            Map<String, Object> p = (params == null) ? Collections.<String, Object>emptyMap() : params;
            resultConsumer.accept(tx.run(call, p).list().stream().map(Record::asMap).collect(Collectors.toList()).iterator());
            tx.success();
            return null;
        });
    }

    public static void testCallInReadTransaction(Session session, String call, Consumer<Map<String, Object>> consumer) {
        testCallInReadTransaction(session, call, null, consumer);
    }

    public static void testCallInReadTransaction(Session session, String call, Map<String,Object> params, Consumer<Map<String, Object>> consumer) {
        testResultInReadTransaction(session, call, params, (res) -> {
            try {
                assertNotNull("result should be not null", res);
                assertTrue("result should be not empty", res.hasNext());
                Map<String, Object> row = res.next();
                consumer.accept(row);
                assertFalse("result should not have next", res.hasNext());
            } catch(Throwable t) {
                printFullStackTrace(t);
                throw t;
            }
        });
    }

    public static void testResultInReadTransaction(Session session, String call, Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        testResultInReadTransaction(session, call, null, resultConsumer);
    }

    public static void testResultInReadTransaction(Session session, String call, Map<String,Object> params, Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        session.readTransaction(tx -> {
            Map<String, Object> p = (params == null) ? Collections.<String, Object>emptyMap() : params;
            resultConsumer.accept(tx.run(call, p).list().stream().map(Record::asMap).collect(Collectors.toList()).iterator());
            tx.success();
            return null;
        });
    }

}
