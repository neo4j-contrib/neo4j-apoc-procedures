package apoc.s3;

import apoc.export.csv.ExportCSV;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.load.LoadCsv;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Map;

import static apoc.ApocConfig.*;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Ignore
public class LoadS3MinioTest {

    private static final String ACCESS_KEY = "testAccessKey";
    private static final String SECRET_KEY = "testSecretKey";
    private static final String BUCKET_NAME = "test";
    private static GenericContainer<?> minioContainer;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();
    
    @BeforeClass
    public static void init() throws Throwable {
        // to make Minio work
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");
        System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
        
        TestUtil.registerProcedure(db, 
                ExportCSV.class, ExportGraphML.class, ExportJson.class, 
                LoadCsv.class, LoadJson.class, Xml.class);

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);

        db.executeTransactionally("CREATE (f:User1:User {name:'foo'})-[:KNOWS]->(b:User {name:'bar'})");

        minioContainer = new GenericContainer<>("bitnami/minio:2025.1.20")
                .withExposedPorts(9000, 9001)
                .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                .withEnv("MINIO_DEFAULT_BUCKETS", BUCKET_NAME)
                .waitingFor(
                        Wait.forLogMessage(".*Bucket created successfully.*\\n", 1)
                                .withStartupTimeout(Duration.ofSeconds(30))
                );

        minioContainer.start();
    }

    @AfterClass
    public static void destroy() {
        System.clearProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation");
        System.clearProperty("com.amazonaws.sdk.disableCertChecking");
        
        minioContainer.close();
    }


    private static String getUrl(String fileName) {
        return String.format(
                "s3://%s:%s/%s/%s?accessKey=%s&secretKey=%s",
                minioContainer.getHost(),
                minioContainer.getMappedPort(9000),
                BUCKET_NAME,
                fileName,
                ACCESS_KEY,
                SECRET_KEY
        );
    }

    @Test
    public void testLoadCsvS3() throws Exception {
        
        String url = getUrl("test.csv");

        testCall(db, "CALL apoc.export.csv.all($url, {})",
                map("url", url),
                r -> assertEquals(url, r.get("file"))
        );

        testResult(db, "CALL apoc.load.csv($url)",
                map("url", url),
                r -> {
                    ResourceIterator<Map> values = r.columnAs("map");
                    Map row = values.next();
                    assertEquals(":User:User1", row.get("_labels"));
                    row = values.next();
                    assertEquals(":User", row.get("_labels"));
                    row = values.next();
                    assertEquals("KNOWS", row.get("_type"));
                    assertFalse(values.hasNext());
                });
    }

    @Test
    public void testLoadJsonS3() throws Exception {
        String url = getUrl("test.json");

        testCall(db, "CALL apoc.export.json.all($url)",
                map("url", url),
                r -> assertEquals(url, r.get("file"))
        );
        
        testResult(db, "CALL apoc.load.json($url,'')",
                map("url", url),
                r -> {
                    ResourceIterator<Map> values = r.columnAs("value");
                    Map row = values.next();
                    assertEquals("node", row.get("type"));
                    row = values.next();
                    assertEquals("node", row.get("type"));
                    row = values.next();
                    assertEquals("relationship", row.get("type"));
                    assertFalse(values.hasNext());
                });
    }

    @Test
    public void testLoadXmlS3() throws Exception {
        String url = getUrl("test.xml");

        testCall(db, "CALL apoc.export.graphml.all($url, {})",
                map("url", url),
                r -> assertEquals(url, r.get("file"))
        );

        testResult(db, "CALL apoc.load.xml($url,'')",
                map("url", url),
                r -> {
                    ResourceIterator<Map> values = r.columnAs("value");
                    Map row = values.next();
                    assertEquals("graphml", row.get("_type"));
                    assertFalse(values.hasNext());
                });
    }


}