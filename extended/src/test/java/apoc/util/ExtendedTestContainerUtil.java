package apoc.util;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.driver.Session;

import static apoc.util.TestContainerUtil.copyFilesToPlugin;
import static apoc.util.TestContainerUtil.executeGradleTasks;

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

    public static void addExtraDependencies() {
        File extraDepsDir = new File(TestContainerUtil.baseDir, "extra-dependencies");
        // build the extra-dependencies
        executeGradleTasks(extraDepsDir, "buildDependencies");

        // add all extra deps to the plugin docker folder
        final File directory = new File(extraDepsDir, "build/allJars");
        final IOFileFilter instance = new WildcardFileFilter("*-all.jar");
        copyFilesToPlugin(directory, instance, TestContainerUtil.pluginsFolder);
    }

}
