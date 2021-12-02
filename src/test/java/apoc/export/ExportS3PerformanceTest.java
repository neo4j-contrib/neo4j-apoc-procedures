package apoc.export;

import apoc.export.csv.ExportCSV;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.s3.S3Aws;
import apoc.util.s3.S3Params;
import apoc.util.s3.S3ParamsExtractor;
import apoc.util.s3.S3TestUtil;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.isTravis;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assume.assumeFalse;

public class ExportS3PerformanceTest {
    private static int REPEAT_TEST = 3;

    private void verifyFileUploaded(String s3Url, String fileName) throws IOException {
        S3Params s3Params = S3ParamsExtractor.extract(new URL(s3Url));
        S3Aws s3Aws = new S3Aws(s3Params, s3Params.getRegion());
        AmazonS3 s3Client = s3Aws.getClient();

        boolean fileUploaded = false;
        ObjectListing objectListing = s3Client.listObjects(s3Params.getBucket());
        for(S3ObjectSummary os : objectListing.getObjectSummaries()) {
            if (os.getKey().equals(fileName)) {
                fileUploaded = true;
                break;
            }
        }
        assertTrue(fileUploaded);
    }

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExportAllCsvS3() throws Exception {
        assumeFalse(isTravis());
        S3TestUtil s3ExportTestUtils = new S3TestUtil();
        try {
            System.out.println("Data creation started.");
            // create data (> 1 MB)
            for (int i = 0; i < 5550; i++) {
                String query = String.format("CREATE (f:User1:User {name:'foo%d',age:%d,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar%d',age:%d}),(c:User {age:12})", i, i, i, i);
                db.execute(query);
            }
            System.out.println("Data creation finished.");

            System.out.println("Test started.");
            for (int repeat = 1; repeat <= REPEAT_TEST; repeat++) {
                String fileName = String.format("performanceTest_%d.csv", repeat);
                String s3Url = s3ExportTestUtils.getUrl(fileName);

                // Run the performance testing
                final Instant start = Instant.now();
                TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,null)",
                        map("s3", s3Url),
                        (r) -> {
                        });
                final Instant end = Instant.now();
                final Duration diff = Duration.between(start, end);
                System.out.println("Time to upload" + ": " + diff.toMillis() + " ms.");

                // Verify the file was successfully uploaded.
                verifyFileUploaded(s3Url, fileName);
            }
            System.out.println("Test finished.");
            s3ExportTestUtils.close();
        } catch (Exception e) {
            s3ExportTestUtils.close();
            throw new Exception(e);
        }
    }
}
