package apoc.hadoop;

import apoc.util.ExtendedTestContainerUtil;
import apoc.util.Neo4jContainerExtension;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.*;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.driver.Session;
import org.testcontainers.containers.*;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
//import org.testcontainers.junit.jupiter.Container;
//import org.testcontainers.junit.jupiter.Testcontainers;
//import org.testcontainers.utility.DockerImageName;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static apoc.util.TestContainerUtil.*;
import static apoc.util.Util.map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

//@Testcontainers
public class HdfsContainerBaseTest {

    public static final String APACHE_HADOOP_IMAGE = "apache/hadoop:3.3.6";
    private static Network hadoopNetwork = Network.newNetwork();

    protected static Neo4jContainerExtension neo4jContainer;
    protected static GenericContainer<?> namenode;
    private static GenericContainer<?> datanode;
    private static GenericContainer<?> resourcemanager;
    private static GenericContainer<?> nodemanager;
    
    
    
//    private static GenericContainer<?> neo4jContainer;
    
    
    // Define the network name from docker-compose.yml
    private static final String NETWORK_NAME = "test_network";
    private static final String HDFS_NAMENODE_HOSTNAME = "namenode";
    private static final int HDFS_RPC_PORT = 8020;
    private static final String CSV_CONTENT = "name,age\nAlice,30\nBob,40";
    public static final String NAMENODE_1 = "namenode-1";
    public static final String hdfsUrl = "hdfs://namenode:8020";

    // Start the HDFS cluster with DockerComposeContainer
    
    public static ComposeContainer hdfsCluster;

    // Start the Neo4j container with APOC and connect it to the same network
//    @Container
//    private static Neo4jContainerExtension neo4jContainer;
    protected static Session session;

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

    @BeforeClass
    public static void setUp() throws Exception {
        // Shared environment variables
//        String hdfsName = "hdfs://namenode:8020";
        String rpcAddress = "namenode:8020";

//        Consumer<CreateContainerCmd> cmdModifier = cmd -> cmd.withHostConfig(
//                new HostConfig().withPortBindings(
//                        PortBinding.parse("0.0.0.0:8020:8020"),
//                        PortBinding.parse("0.0.0.0:9870:9870")
//                )
//        );

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
//        Consumer<CreateContainerCmd> cmdModifier4 = cmd -> {
//            Ports portBindings = new Ports();
//            portBindings.bind(ExposedPort.tcp(8020), Ports.Binding.bindPort(8020));
//            portBindings.bind(ExposedPort.tcp(9870), Ports.Binding.bindPort(9870));
//            cmd.getHostConfig().withPortBindings(portBindings);
//        };
        
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
//                .waitingFor(Wait.forLogMessage(".*NameNode RPC up at:.*\\n", 1));

        // Datanode
        datanode = new GenericContainer<>(DockerImageName.parse(APACHE_HADOOP_IMAGE))
                .withNetwork(hadoopNetwork)
                .withCommand("hdfs", "datanode")
                .withEnv("CORE-SITE.XML_fs.default.name", hdfsUrl)
                .withEnv("CORE-SITE.XML_fs.defaultFS", hdfsUrl)
                .withEnv("HDFS-SITE.XML_dfs.namenode.rpc-address", rpcAddress)
                .withEnv("HADOOP_USER_NAME", "hadoop")
                .withExposedPorts(9866, 9864)
//                .waitingFor(Wait.forLogMessage(".*DataNode is active.*\\n", 1))
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
//                .waitingFor(Wait.forLogMessage(".*ResourceManager is started.*\\n", 1))
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
//                .waitingFor(Wait.forLogMessage(".*NodeManager is started.*\\n", 1))
                .dependsOn(namenode, resourcemanager);
        
        
//        neo4jContainer = new GenericContainer<>(DockerImageName.parse("neo4j:5.26.0"))
//                .withNetwork(hadoopNetwork)
//                .withNetworkAliases("neo4j")
//                .withExposedPorts(7474, 7687)
//                .withEnv("NEO4J_AUTH", "neo4j/password");
        
        // TODO - put this try in PR
//        File dockerComposeFile = new File("src/test/resources/docker-compose.yml");
//        hdfsCluster = new ComposeContainer(dockerComposeFile)
////                .withLocalCompose(true)
//                .withExposedService(NAMENODE_1, 8020)
//                .withExposedService("datanode-1", 9864)
//                .withExposedService("resourcemanager-1", 8088)
//        
////                .withExposedService("nodemanager-1", 8024)
//        ;
//                .withExposedService(HDFS_NAMENODE_HOSTNAME, HDFS_RPC_PORT, Wait.forListeningPort());
//        hdfsCluster.start();
        
//        hdfsCluster.
//                .withNetwork(NETWORK_NAME)


//        waitForNetwork("hadoop-neo4j-net");

//        Map<String, ContainerNetwork> networks = hdfsCluster.getContainerByServiceName(NAMENODE_1)
//                .get().getContainerInfo().getNetworkSettings().getNetworks();
        

        // TODO - put this try in PR
        neo4jContainer = new Neo4jContainerExtension(neo4jCommunityDockerImageVersion, Files.createTempDirectory("neo4j-logs"))
//        neo4jContainer = (Neo4jContainer) new Neo4jContainer(neo4jCommunityDockerImageVersion)
                .withNetwork(hadoopNetwork)
                .withNetworkAliases("neo4j")
                .withEnv("NEO4J_AUTH", "neo4j/password")
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .withEnv("HADOOP_USER_NAME", "hadoop") // Aggiungi questa linea
//                .withEnv("HADOOP_USER_NAME", "root") // Aggiungi questa linea
                .withEnv("apoc.export.file.enabled", "true")
                .withEnv("apoc.import.file.enabled", "true")
                .withNeo4jConfig("dbms.security.procedures.unrestricted", "apoc.*")
                .withPlugins(MountableFile.forHostPath(pluginsFolder.toPath()))
//                .withExposedPorts(7474, 7687)
        ;

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

//           neo4jContainer = createCommunityDB(List.of(TestContainerUtil.ApocPackage.EXTENDED, TestContainerUtil.ApocPackage.CORE), true)
//                   .withInitScript("init_neo4j_bolt.cypher")
////                   .withExposedPorts(7687, 7473, 7474)
//                   .withEnv("HADOOP_USER_NAME", "root")
//                   .withNetwork(hadoopNetwork)
//                   .withNetworkAliases("neo4j")
//                   .withEnv("HADOOP_USER_NAME", "root")
////                   .withCreateContainerCmdModifier(createPortBindingModifier(7474, 7687))
//        ;
        // todo - copy plugins???
        
        
//        neo4jContainer.withNetworkAliases("neo4j").withNetworkMode(networks.keySet().iterator().next());

        ExtendedTestContainerUtil.addExtraDependencies();
        neo4jContainer.start();
        namenode.start();
        datanode.start();
        resourcemanager.start();
        nodemanager.start();
//        neo4j.start();
////        TestUtil.registerProcedure(db, Bolt.class, ExportCypher.class, Cypher.class, PathExplorer.class, GraphRefactoring.class);
////        BOLT_URL = getBoltUrl().replaceAll("'", "");
        session = neo4jContainer.getSession();

        // Ora che il container HDFS è avviato, puoi eseguire i comandi
        // Copia il file di test nel container HDFS
        File tempCsvFile = File.createTempFile("data", ".csv");
        Files.write(tempCsvFile.toPath(), CSV_CONTENT.getBytes());



//        System.out.println("execResult.getStderr() = " + execResult2.getStderr());
//        System.out.println("execResult.getStdout() = " + execResult2.getStdout());

        

        Files.deleteIfExists(tempCsvFile.toPath());
    }

    @AfterClass
    public static void afterClass() throws Exception {
//        hdfsCluster.close();
        neo4jContainer.close();
        namenode.close();
        datanode.close();
        resourcemanager.close();
        nodemanager.close();
    }
    
//    public static Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:5.12"))
//            .withAdminPassword("password")
//            .withPlugins(DockerImageName.parse("neo4j/apoc:5.12.0"))
//            .withNetworkAliases("neo4j") // Optional: a nice alias for the container
//            .withNetworkMode(hdfsCluster.getNetwork().get().getName()); // Connect to the same network

//    @Test
//    public void testApocLoadCsvFromHdfs() throws Exception {
//        // 1. Copy the test CSV file into the HDFS container
////        File tempCsvFile = File.createTempFile("data", ".csv");
////        Files.write(tempCsvFile.toPath(), CSV_CONTENT.getBytes());
//
////        hdfsCluster.execInContainer("hdfs", "dfs", "-put", tempCsvFile.getAbsolutePath(), "/data.csv");
//        
//        testCall(session, "CALL apoc.load.csv($url) YIELD map", 
//                map("url", hdfsName + "/DATA/data.csv"),
//                r -> {
//                    Map<String, String> expected = map("a", "1", "b", "2");
//                    assertEquals(expected, r.get("map"));
//        });
//
//        // 2. Execute the APOC query from Neo4j to HDFS
////        try (Driver driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", "password"));
////             Session session = driver.session()) {
////
////
////            
////            // The Neo4j container can now resolve 'namenode' because they are on the same network
////            String hdfsUrl = String.format("hdfs://%s:%d/data.csv", HDFS_NAMENODE_HOSTNAME, HDFS_RPC_PORT);
////            System.out.println("Neo4j connecting to HDFS via URL: " + hdfsUrl);
////
////            session.run(String.format("CALL apoc.load.csv('%s') YIELD map as row RETURN row", hdfsUrl));
////        }
////
////        // 3. Verify that the data was imported
////        try (Driver driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", "password"));
////             Session session = driver.session()) {
////
////            var result = session.run("MATCH (p) RETURN count(p) as count");
////            assertTrue(result.single().get("count").asInt() > 0, "Data should be loaded into Neo4j");
////        }
//    }
}