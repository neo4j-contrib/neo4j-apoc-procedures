package apoc;

import apoc.load.LoadCsv;
import apoc.load.partial.LoadPartial;
import apoc.util.HdfsTestUtils;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.TestUtil.testResult;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApocHdfsTest {

    private static final String CSV_CONTENT = "name,age\nAlice,30\nBob,40";
    private static final String HADOOP_IMAGE = "apache/hadoop:3.4.1";
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, LoadPartial.class);
    }
    
    @Test
    public void testApocLoadCsvFromHdfs() throws Exception {

        // 1. Avvia un container HDFS
        try (GenericContainer<?> hdfsContainer = new GenericContainer<>(DockerImageName.parse(HADOOP_IMAGE))) {
            hdfsContainer.withExposedPorts(8020, 9870); // NameNode RPC e Web UI
            hdfsContainer.start();

            String hdfsNameNodeUri = "hdfs://" + hdfsContainer.getHost() + ":" + hdfsContainer.getMappedPort(8020);
            System.out.println("HDFS NameNode URI: " + hdfsNameNodeUri);

            // Crea un file CSV temporaneo per il test
            File tempCsvFile = File.createTempFile("data", ".csv");
            Files.write(tempCsvFile.toPath(), CSV_CONTENT.getBytes());

            // Copia il file nel container HDFS usando il comando 'hdfs dfs -put'
            hdfsContainer.execInContainer("hdfs", "dfs", "-put", tempCsvFile.getAbsolutePath(), "/data.csv");
            System.out.println("File copiato su HDFS: /data.csv");

            // 2. Avvia un container Neo4j con APOC
//            try (Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:5.12"))
//                    .withAdminPassword("password")
//                    .withPlugins(DockerImageName.parse("neo4j/apoc:5.12.0"))) {
//                neo4jContainer.start();

                // 3. Connessione a Neo4j e esecuzione del test
//                try (Driver driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", "password"));
//                     Session session = driver.session()) {

                    // Esegui la query APOC. Assicurati che il tuo Neo4j possa connettersi a hdfsContainer.
                    // Testcontainers gestisce la rete tra i container, ma a volte potresti aver bisogno
                    // di configurare la rete esplicitamente per casi più complessi.
                    String query = "CALL apoc.load.csv('" + hdfsNameNodeUri + "/data.csv') " +
                            "YIELD map as person " +
                            "RETURN person.name AS name";

            testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", 
                    Util.map("url", hdfsNameNodeUri,
                            System.getProperty("user.name"), "test.csv"), // 'hdfs://localhost:12345/user/<sys_user_name>/test.csv'
                    (r) -> {
//                        assertRow(r,0L,"name","Selma","age","8");
//                        assertRow(r,1L,"name","Rana","age","11");
//                        assertRow(r,2L,"name","Selina","age","18");
                        assertEquals(false, r.hasNext());
                    });
            
//                    Result result = db.executeTransactionally(query);
//                    var names = result.stream()
//                            .map(r -> r.get("name").asString())
//                            .collect(Collectors.toList());
//
//                    // 4. Verifica il risultato
//                    assertEquals(2, names.size());
//                    assertEquals("Alice", names.get(0));
//                    assertEquals("Bob", names.get(1));
//                    System.out.println("Test di importazione riuscito!");
//                }
//            } finally {
                // Pulizia del file temporaneo
                Files.deleteIfExists(tempCsvFile.toPath());
//            }
        }
    }
}