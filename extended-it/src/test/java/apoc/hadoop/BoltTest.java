package apoc.hadoop;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
//import org.testcontainers.junit.jupiter.Container;
//import org.testcontainers.junit.jupiter.Testcontainers;
//import org.testcontainers.utility.DockerImageName;
import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.jupiter.api.Assertions.assertTrue;

//@Testcontainers
public class BoltTest {

    // Define the network name from docker-compose.yml
    private static final String NETWORK_NAME = "test_network";
    private static final String HDFS_NAMENODE_HOSTNAME = "namenode";
    private static final int HDFS_RPC_PORT = 8020;
    private static final String CSV_CONTENT = "name,age\nAlice,30\nBob,40";

    // Start the HDFS cluster with DockerComposeContainer
    
    public static ComposeContainer hdfsCluster;

    private static Network testNetwork = Network.newNetwork();


    // Start the Neo4j container with APOC and connect it to the same network
//    @Container
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void setUp() throws Exception {
        File dockerComposeFile = new File("src/test/resources/docker-compose.yml");
//        hdfsCluster = new ComposeContainer(dockerComposeFile)
//                .withLocalCompose(true)
//                .withExposedService("namenode-1", 8020)
//                .withExposedService("datanode-1", 9864)
//                .withExposedService("resourcemanager-1", 8088)
////                .withExposedService("nodemanager-1", 8024)
//        ;
////                .withExposedService(HDFS_NAMENODE_HOSTNAME, HDFS_RPC_PORT, Wait.forListeningPort());
//        hdfsCluster.start();
//        hdfsCluster.
//                .withNetwork(NETWORK_NAME)
        
        
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED, TestContainerUtil.ApocPackage.CORE), true).withInitScript("init_neo4j_bolt.cypher");
//        neo4jContainer.withNetworkAliases("neo4j").withNetworkMode("hadoop-neo4j-net");
        neo4jContainer.start();
//        TestUtil.registerProcedure(db, Bolt.class, ExportCypher.class, Cypher.class, PathExplorer.class, GraphRefactoring.class);
//        BOLT_URL = getBoltUrl().replaceAll("'", "");
        session = neo4jContainer.getSession();

        // Ora che il container HDFS è avviato, puoi eseguire i comandi
        // Copia il file di test nel container HDFS
        File tempCsvFile = File.createTempFile("data", ".csv");
        Files.write(tempCsvFile.toPath(), CSV_CONTENT.getBytes());

        // Esegui il comando di copia dopo l'avvio del container
//        hdfsCluster.getService(HDFS_NAMENODE_HOSTNAME, HDFS_RPC_PORT)
//                .execInContainer("hdfs", "dfs", "-put", tempCsvFile.getAbsolutePath(), "/data.csv");

        System.out.println("File copiato su HDFS: /data.csv");

        Files.deleteIfExists(tempCsvFile.toPath());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        hdfsCluster.close();
        neo4jContainer.close();
    }
    
//    public static Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:5.12"))
//            .withAdminPassword("password")
//            .withPlugins(DockerImageName.parse("neo4j/apoc:5.12.0"))
//            .withNetworkAliases("neo4j") // Optional: a nice alias for the container
//            .withNetworkMode(hdfsCluster.getNetwork().get().getName()); // Connect to the same network

    @Test
    public void testApocLoadCsvFromHdfs() throws Exception {
        // 1. Copy the test CSV file into the HDFS container
        File tempCsvFile = File.createTempFile("data", ".csv");
        Files.write(tempCsvFile.toPath(), CSV_CONTENT.getBytes());

//        hdfsCluster.execInContainer("hdfs", "dfs", "-put", tempCsvFile.getAbsolutePath(), "/data.csv");
        System.out.println("File copied to HDFS: /data.csv");

        // 2. Execute the APOC query from Neo4j to HDFS
        try (Driver driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", "password"));
             Session session = driver.session()) {

            // The Neo4j container can now resolve 'namenode' because they are on the same network
            String hdfsUrl = String.format("hdfs://%s:%d/data.csv", HDFS_NAMENODE_HOSTNAME, HDFS_RPC_PORT);
            System.out.println("Neo4j connecting to HDFS via URL: " + hdfsUrl);

            session.run(String.format("CALL apoc.load.csv('%s') YIELD map as row RETURN row", hdfsUrl));
        }

        // 3. Verify that the data was imported
        try (Driver driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", "password"));
             Session session = driver.session()) {

            var result = session.run("MATCH (p) RETURN count(p) as count");
            assertTrue(result.single().get("count").asInt() > 0, "Data should be loaded into Neo4j");
        }
    }
}