package apoc.util;

import com.github.dockerjava.api.exception.NotFoundException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.testcontainers.containers.ContainerFetchException;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.printFullStackTrace;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestContainerUtil {
    public enum Neo4jVersion {
        ENTERPRISE,
        COMMUNITY
    }

    public enum ApocPackage {
        CORE,
        EXTENDED
    }

    // read neo4j version from build.gradle
    public static final String neo4jEnterpriseDockerImageVersion = System.getProperty("neo4jDockerImage");
    public static final String neo4jCommunityDockerImageVersion = System.getProperty("neo4jCommunityDockerImage");

    public static final String password = "apoc";

    private TestContainerUtil() {}

    private static File baseDir = Paths.get("..").toFile();
    private static File coreDir = new File(baseDir, "core");
    private static File extendedDir = new File(baseDir, "extended");

    public static TestcontainersCausalCluster createEnterpriseCluster(List<ApocPackage> apocPackages, int numOfCoreInstances, int numberOfReadReplica, Map<String, Object> neo4jConfig, Map<String, String> envSettings) {
        return TestcontainersCausalCluster.create(apocPackages, numOfCoreInstances, numberOfReadReplica, Duration.ofMinutes(4), neo4jConfig, envSettings);
    }

    public static Neo4jContainerExtension createDB(Neo4jVersion version, List<ApocPackage> apocPackages, boolean withLogging) {
        return switch(version) {
            case ENTERPRISE -> createEnterpriseDB(apocPackages, withLogging);
            case COMMUNITY -> createCommunityDB(apocPackages, withLogging);
        };
    }

    public static Neo4jContainerExtension createEnterpriseDB(List<ApocPackage> apocPackages, boolean withLogging) {
        return createNeo4jContainer(apocPackages, withLogging, Neo4jVersion.ENTERPRISE);
    }

    public static Neo4jContainerExtension createCommunityDB(List<ApocPackage> apocPackages, boolean withLogging) {
        return createNeo4jContainer(apocPackages, withLogging, Neo4jVersion.COMMUNITY);
    }

    private static Neo4jContainerExtension createNeo4jContainer(List<ApocPackage> apocPackages, boolean withLogging, Neo4jVersion version) {
        String dockerImage;
        if (version == Neo4jVersion.ENTERPRISE) {
            dockerImage = neo4jEnterpriseDockerImageVersion;
        } else {
            dockerImage = neo4jCommunityDockerImageVersion;
        }

        File pluginsFolder = new File(baseDir, "build/plugins");
        try {
            FileUtils.deleteDirectory( pluginsFolder );
        } catch (IOException e) {
            e.printStackTrace();
        }
        File projectDir;
        // We define the container with external volumes
        File importFolder = new File("import");
        importFolder.mkdirs();
        // use a separate folder for mounting plugins jar - build/libs might contain other jars as well.
        pluginsFolder.mkdirs();
        String canonicalPath = null;

        try {
            canonicalPath = importFolder.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (ApocPackage apocPackage: apocPackages) {
            if (apocPackage == ApocPackage.CORE) {
                projectDir = coreDir;
            } else {
                projectDir = extendedDir;
            }

            executeGradleTasks(projectDir, "shadowJar");

            Collection<File> files = FileUtils.listFiles(new File(projectDir, "build/libs"), new WildcardFileFilter(Arrays.asList("*-extended.jar", "*-core.jar")), null);
            for (File file: files) {
                try {
                    FileUtils.copyFileToDirectory(file, pluginsFolder);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        System.out.println("neo4jDockerImageVersion = " + dockerImage);
        Neo4jContainerExtension neo4jContainer = new Neo4jContainerExtension(dockerImage)
                .withPlugins(MountableFile.forHostPath(pluginsFolder.toPath()))
                .withTmpFs(Map.of("/logs", "rw", "/data", "rw", pluginsFolder.toPath().toAbsolutePath().toString(), "rw"))
                .withAdminPassword(password)
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .withEnv("apoc.export.file.enabled", "true")
                .withNeo4jConfig("dbms.memory.heap.max_size", "512M")
                .withNeo4jConfig("dbms.memory.pagecache.size", "256M")
                .withNeo4jConfig("dbms.security.procedures.unrestricted", "apoc.*")
                .withNeo4jConfig("dbms.logs.http.enabled", "true")
                .withNeo4jConfig("dbms.logs.debug.level", "DEBUG")
                .withNeo4jConfig("dbms.routing.driver.logging.level", "DEBUG")
                .withFileSystemBind(canonicalPath, "/var/lib/neo4j/import") // map the "target/import" dir as the Neo4j's import dir
                .withCreateContainerCmdModifier(cmd -> cmd.withMemory(2024 * 1024 * 1024L)) // 2gb
                .withExposedPorts(7687, 7473, 7474)
//                .withDebugger()  // attach debugger

                // set uid if possible - export tests do write to "/import"
                .withCreateContainerCmdModifier(cmd -> {
                    try {
                        Process p = Runtime.getRuntime().exec("id -u");
                        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        String s = br.readLine();
                        p.waitFor();
                        p.destroy();
                        cmd.withUser(s);
                    } catch (Exception e) {
                        System.out.println("Exception while assign cmd user to docker container:\n" + ExceptionUtils.getStackTrace(e));
                        // ignore since it may fail depending on operating system
                    }
                });

        if (withLogging) {
            neo4jContainer.withLogging();
        }

        return neo4jContainer.withWaitForNeo4jDatabaseReady(password, version);
    }

    public static void executeGradleTasks(File baseDir, String... tasks) {
        try (ProjectConnection connection = GradleConnector.newConnector()
                .forProjectDirectory(baseDir)
                .useBuildDistribution()
                .connect()) {
//            String version = connection.getModel(ProjectPublications.class).getPublications().getAt(0).getId().getVersion();

            BuildLauncher buildLauncher = connection.newBuild().forTasks(tasks);

            String neo4jVersionOverride = System.getenv("NEO4JVERSION");
            System.out.println("neo4jVersionOverride = " + neo4jVersionOverride);
            if (neo4jVersionOverride != null) {
                buildLauncher = buildLauncher.addArguments("-P", "neo4jVersionOverride=" + neo4jVersionOverride);
            }

            String localMaven = System.getenv("LOCAL_MAVEN");
            System.out.println("localMaven = " + localMaven);
            if (localMaven != null) {
                buildLauncher = buildLauncher.addArguments("-D", "maven.repo.local=" + localMaven);
            }

            buildLauncher.run();
        }
    }

    public static void executeGradleTasks(String... tasks) {
        executeGradleTasks(baseDir, tasks);
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
            tx.commit();
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
            tx.commit();
            return null;
        });
    }

    public static <T> T singleResultFirstColumn(Session session, String cypher, Map<String,Object> params) {
        return (T) session.writeTransaction(tx -> tx.run(cypher, params).single().fields().get(0).value().asObject());
    }

    public static <T> T singleResultFirstColumn(Session session, String cypher) {
        return singleResultFirstColumn(session, cypher, Map.of());
    }

    public static boolean isDockerImageAvailable(Exception ex) {
        final Throwable cause = ex.getCause();
        final Throwable rootCause = ExceptionUtils.getRootCause(ex);
        return !(cause instanceof ContainerFetchException && rootCause instanceof NotFoundException);
    }

}
