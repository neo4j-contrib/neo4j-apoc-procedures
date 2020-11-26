package apoc.export;

import apoc.ApocSettings;
import apoc.export.csv.ExportCSV;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.s3.S3Aws;
import apoc.util.s3.S3Params;
import apoc.util.s3.S3ParamsExtractor;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Ignore;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static apoc.util.MapUtil.map;
import static junit.framework.TestCase.assertTrue;

@Ignore("To use this test, you need to set the S3 bucket and region to a valid endpoint " +
        "and have your access key and secret key setup in your environment.")
public class ExportS3PerformanceTest {
    private static String S3_BUCKET_NAME = null;
    private static int REPEAT_TEST = 3;

    private static String getEnvVar(String envVarKey) throws Exception {
        return Optional.ofNullable(System.getenv(envVarKey)).orElseThrow(
                () -> new Exception(String.format("%s is not set in the environment", envVarKey))
        );
    }

    private static String getS3Url(String key) {
        return String.format("s3://:/%s/%s", S3_BUCKET_NAME, key);
    }

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

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_export_file_enabled, true);

    @BeforeClass
    public static void setUp() throws Exception {
        if (S3_BUCKET_NAME == null) {
            S3_BUCKET_NAME = getEnvVar("S3_BUCKET_NAME");
        }
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);
    }

    @Test
    public void testExportAllCsvS3() throws Exception {
        System.out.println("Data creation started.");
        // create large data (> 100 MB)
        for (int i=0; i<555000; i++) {
            String query = String.format("CREATE (f:User1:User {name:'foo%d',age:%d,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar%d',age:%d}),(c:User {age:12})", i, i, i, i );
            db.executeTransactionally(query);
        }
        System.out.println("Data creation finished.");

        System.out.println("Test started.");
        for (int repeat=1; repeat<=REPEAT_TEST; repeat++) {
            String fileName = String.format("performanceTest_%d.csv", repeat);
            String s3Url = getS3Url(fileName);

            // Run the performance testing
            final Instant start = Instant.now();
            TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,null)",
                    map("s3", s3Url),
                    (r) -> {});
            final Instant end = Instant.now();
            final Duration diff = Duration.between(start, end);
            System.out.println("Time to upload" + ": " + diff.toMillis() + " ms.");

            // Verify the file was successfully uploaded.
            verifyFileUploaded(s3Url, fileName);
        }
        System.out.println("Test finished.");
    }
}
