package apoc.util.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import org.neo4j.test.assertion.Assert;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Utility class for testing Amazon S3 related functionality.
 */
public class S3TestUtil {

    /**
     * Read file object as a string from S3 bucket. This code expects valid AWS credentials are set up.
     * @param s3Url String containing url to S3 bucket.
     * @return the s3 string object
     */
    public static String readS3FileToString(String s3Url) throws AmazonClientException {
        try {
            S3Object s3object = getS3Object(s3Url);
            S3ObjectInputStream inputStream = s3object.getObjectContent();
            return IOUtils.toString(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static S3Object getS3Object(String s3Url) throws MalformedURLException, AmazonClientException {
        S3Params s3Params = S3ParamsExtractor.extract(new URL(s3Url));
        S3Aws s3Aws = new S3Aws(s3Params, s3Params.getRegion());
        AmazonS3 s3Client = s3Aws.getClient();
        
        return s3Client.getObject(s3Params.getBucket(), s3Params.getKey());
    }

    public static void assertStringFileEquals(String expected, String s3Url) {
        Assert.assertEventually(() -> {
            final String actual;
            try {
                actual = readS3FileToString(s3Url);
            } catch (AmazonClientException e) {
                if (e.getMessage().contains("The specified key does not exist")) {
                    return false;
                }
                throw e;
            }
            assertEquals(expected, actual);
            return true;
        }, v -> v, 30L, TimeUnit.SECONDS);
    }
}
