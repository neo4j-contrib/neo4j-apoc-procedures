package apoc.util;

import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Paths;
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



    public static Neo4jContainerExtension createEnterpriseDB(boolean withLogging) {
        // We build the project, the artifact will be placed into ./build/libs
        executeGradleTasks("clean", "shadow");
        // We define the container with external volumes
        Neo4jContainerExtension neo4jContainer = new Neo4jContainerExtension()
                .withPlugins(MountableFile.forHostPath("./target/tests/gradle-build/libs")) // map the apoc's artifact dir as the Neo4j's plugin dir
                .withoutAuthentication()
                .withEnv("NEO4J_apoc_export_file_enabled", "true")
                .withEnv("NEO4J_dbms_security_procedures_unrestricted", "apoc.*")
//                .withEnv("NEO4J_wrapper_java_additional","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -Xdebug-Xnoagent-Djava.compiler=NONE-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005")
                .withFileSystemBind("./target/import", "/import") // map the "target/import" dir as the Neo4j's import dir
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes");
        if (withLogging) {
            neo4jContainer.withLogging();
        }
        neo4jContainer.setDockerImageName("neo4j:3.5.3-enterprise");
        return neo4jContainer;
    }

    private static void executeGradleTasks(String... tasks) {
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

}
