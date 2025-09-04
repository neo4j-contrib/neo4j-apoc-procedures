package apoc.hadoop;

import apoc.util.Neo4jContainerExtension;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.driver.Session;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static apoc.util.TestContainerUtil.*;

/**
 * Create a TestContainer's network equivalent of the `src/test/resources/docker-compose-hadoop.yml`
 */
public class HdfsContainerBaseTest {

    public static final String APACHE_HADOOP_IMAGE = "apache/hadoop:3.3.6";
    public static final String hdfsUrl = "hdfs://namenode:8020";

    private static Network hadoopNetwork = Network.newNetwork();

    protected static Neo4jContainerExtension neo4jContainer;
    protected static GenericContainer<?> namenode;
    private static GenericContainer<?> datanode;
    private static GenericContainer<?> resourcemanager;
    private static GenericContainer<?> nodemanager;
    

    // Start the HDFS cluster with DockerComposeContainer
    protected static Session session;
    

    @BeforeClass
    public static void setUp() throws Exception {
        String rpcAddress = "namenode:8020";

        Consumer<CreateContainerCmd> cmdModifier = cmd -> {
            Ports portBindings = new Ports();
            portBindings.bind(ExposedPort.tcp(8020), Ports.Binding.bindPort(8020));
            portBindings.bind(ExposedPort.tcp(9870), Ports.Binding.bindPort(9870));
            cmd.getHostConfig().withPortBindings(portBindings);
        };
        Consumer<CreateContainerCmd> cmdModifier2 = cmd -> {
            Ports portBindings = new Ports();
            portBindings.bind(ExposedPort.tcp(9866), Ports.Binding.bindPort(9866));
            portBindings.bind(ExposedPort.tcp(9864), Ports.Binding.bindPort(9864));
            cmd.getHostConfig().withPortBindings(portBindings);
        };
        Consumer<CreateContainerCmd> cmdModifier3 = cmd -> {
            Ports portBindings = new Ports();
            portBindings.bind(ExposedPort.tcp(8088), Ports.Binding.bindPort(8088));
            cmd.getHostConfig().withPortBindings(portBindings);
        };
        
        // Namenode
        namenode = new GenericContainer<>(DockerImageName.parse(APACHE_HADOOP_IMAGE))
                .withNetwork(hadoopNetwork)
                .withNetworkAliases("namenode")
                .withCommand("hdfs", "namenode")
                .withEnv("CORE-SITE.XML_fs.default.name", hdfsUrl)
                .withEnv("CORE-SITE.XML_fs.defaultFS", hdfsUrl)
                .withEnv("HDFS-SITE.XML_dfs.namenode.rpc-address", rpcAddress)
                .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-root/dfs/name")
                .withEnv("HADOOP_USER_NAME", "hadoop")
                .withCreateContainerCmdModifier(cmdModifier);

        // Datanode
        datanode = new GenericContainer<>(DockerImageName.parse(APACHE_HADOOP_IMAGE))
                .withNetwork(hadoopNetwork)
                .withCommand("hdfs", "datanode")
                .withEnv("CORE-SITE.XML_fs.default.name", hdfsUrl)
                .withEnv("CORE-SITE.XML_fs.defaultFS", hdfsUrl)
                .withEnv("HDFS-SITE.XML_dfs.namenode.rpc-address", rpcAddress)
                .withEnv("HADOOP_USER_NAME", "hadoop")
                .withExposedPorts(9866, 9864)
                .dependsOn(namenode)
                .withCreateContainerCmdModifier(cmdModifier2);

        // ResourceManager
        resourcemanager = new GenericContainer<>(DockerImageName.parse(APACHE_HADOOP_IMAGE))
                .withNetwork(hadoopNetwork)
                .withNetworkAliases("resourcemanager")
                .withCommand("yarn", "resourcemanager")
                .withEnv("CORE-SITE.XML_fs.default.name", hdfsUrl)
                .withEnv("CORE-SITE.XML_fs.defaultFS", hdfsUrl)
                .withEnv("HDFS-SITE.XML_dfs.namenode.rpc-address", rpcAddress)
                .withEnv("HADOOP_USER_NAME", "hadoop")
                .withExposedPorts(8088)
                .dependsOn(namenode)
                .withCreateContainerCmdModifier(cmdModifier3);

        // NodeManager
        nodemanager = new GenericContainer<>(DockerImageName.parse(APACHE_HADOOP_IMAGE))
                .withNetwork(hadoopNetwork)
                .withCommand("yarn", "nodemanager")
                .withEnv("CORE-SITE.XML_fs.default.name", hdfsUrl)
                .withEnv("CORE-SITE.XML_fs.defaultFS", hdfsUrl)
                .withEnv("HDFS-SITE.XML_dfs.namenode.rpc-address", rpcAddress)
                .withEnv("HADOOP_USER_NAME", "hadoop")
                .dependsOn(namenode, resourcemanager);

        // TODO - put this try in PR
        neo4jContainer = new Neo4jContainerExtension(neo4jCommunityDockerImageVersion, Files.createTempDirectory("neo4j-logs"))
                .withNetwork(hadoopNetwork)
                .withNetworkAliases("neo4j")
                .withEnv("NEO4J_AUTH", "neo4j/password")
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .withEnv("HADOOP_USER_NAME", "hadoop")
                .withEnv("apoc.export.file.enabled", "true")
                .withEnv("apoc.import.file.enabled", "true")
                .withNeo4jConfig("dbms.security.procedures.unrestricted", "apoc.*")
                .withPlugins(MountableFile.forHostPath(pluginsFolder.toPath()));

        executeGradleTasks(extendedDir, "shadowJar");
        copyFilesToPlugin(
                new File(extendedDir, "build/libs"),
                new WildcardFileFilter(Arrays.asList("*-extended.jar", "*-core.jar")),
                pluginsFolder);

        File coreDir = new File(baseDir, System.getProperty("coreDir"));
        executeGradleTasks(coreDir, "shadowJar");
        copyFilesToPlugin(
                new File(coreDir, "build/libs"),
                new WildcardFileFilter(Arrays.asList("*-extended.jar", "*-core.jar")),
                pluginsFolder);
        
        neo4jContainer.setExposedPorts(List.of(7474, 7687));
        
        neo4jContainer.start();
        namenode.start();
        datanode.start();
        resourcemanager.start();
        nodemanager.start();
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        neo4jContainer.close();
        namenode.close();
        datanode.close();
        resourcemanager.close();
        nodemanager.close();
    }
}