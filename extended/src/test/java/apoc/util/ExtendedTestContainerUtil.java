package apoc.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;


import static apoc.util.ExtendedUtil.retryRunnable;
//import static apoc.util.TestContainerUtil.copyFilesToPlugin;
//import static apoc.util.TestContainerUtil.executeGradleTasks;
import static apoc.util.TestContainerUtil.pluginsFolder;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class ExtendedTestContainerUtil
{
    public static File baseDir = Paths.get("..").toFile();
    private static File coreDir = new File(baseDir, System.getProperty("coreDir"));
    public static File extendedDir = new File(baseDir, "extended");
    
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
        String jarPathProp = "apoc-extra-dependencies.test.jar.path";
        String property = System.getProperty(jarPathProp);
        final var jarPath = Path.of(property);
        final var destination = pluginsFolder.toPath();//.resolve(jarPath.getFileName());
        try {
            System.out.println("Copying %s (prop %s) => %s".formatted(jarPath, jarPathProp, destination));
            Files.createDirectories(pluginsFolder.toPath());
            File file = jarPath.toFile();
            File file1 = destination.toFile();
            if (!file.exists() || !file1.exists()) {
                // TODO ... error message
                throw new RuntimeException("Folder to copy %s to %s");
            }
            System.out.println("org.apache.commons.io.FileUtils.listFiles(file, instance, null); = " + FileUtils.listFiles(file, FileFileFilter.INSTANCE, null));
            System.out.println("org.apache.commons.io.FileUtils.listFiles(file1, instance, null); = " + FileUtils.listFiles(file1, FileFileFilter.INSTANCE, null));
                    
                    
            copyFilesToFolder(file, FileFileFilter.INSTANCE, file1);

            ;
            System.out.println("destination = " + destination);
//            Files.copy(jarPath, destination, REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy %s to %s".formatted(jarPath, destination), e);
        }
        
//        // build the extra-dependencies
//        executeGradleTasks(extraDepsDir, "buildDependencies");
//
//        // add all extra deps to the plugin docker folder
//        final File directory = new File(extraDepsDir, "build/allJars");
//        final IOFileFilter instance = new WildcardFileFilter("*.jar");
//        copyFilesToPlugin(directory, instance, TestContainerUtil.pluginsFolder);
    }

    // TODO - SPOSTARLO IN ExtendedUtil e dire che l'ho generalizzato chiamandolo copy files to folder
    //  visto che è stato rimosso da core
    public static void copyFilesToFolder(File directory, IOFileFilter instance, File targetFolder) {
        Collection<File> files = org.apache.commons.io.FileUtils.listFiles(directory, instance, null);
        for (File file : files) {
            System.out.println("file = " + file);
            try {
                FileUtils.copyFileToDirectory(file, targetFolder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

//    public static void executeGradleTasks(File baseDir, String... tasks) {
//        try (ProjectConnection connection = GradleConnector.newConnector()
//                .forProjectDirectory(baseDir)
//                .useBuildDistribution()
//                .connect()) {
//            BuildLauncher buildLauncher = connection.newBuild().forTasks(tasks);
//
//            String neo4jVersionOverride = System.getenv("NEO4JVERSION");
//            System.out.println("neo4jVersionOverride = " + neo4jVersionOverride);
//            if (neo4jVersionOverride != null) {
//                buildLauncher = buildLauncher.addArguments("-P", "neo4jVersionOverride=" + neo4jVersionOverride);
//            }
//
//            String localMaven = System.getenv("LOCAL_MAVEN");
//            System.out.println("localMaven = " + localMaven);
//            if (localMaven != null) {
//                buildLauncher = buildLauncher.addArguments("-D", "maven.repo.local=" + localMaven);
//            }
//
//            buildLauncher.run();
//        }
//    }

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

}
