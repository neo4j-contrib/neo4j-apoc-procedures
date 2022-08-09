package apoc.util.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Utility class for testing Amazon S3 related functionality.
 */
public class S3TestUtil {

    /**
     * Read object from S3 bucket and save it as a file. This code expects valid AWS credentials are set up.
     * @param s3Url String containing url to S3 bucket.
     * @param pathname Local pathname where the file will be stored.
     * @throws IOException Exception thrown if copying stream to file fails.
     */
    public static void readFile(String s3Url, String pathname) throws IOException {
        S3Params s3Params = S3ParamsExtractor.extract(new URL(s3Url));
        S3Aws s3Aws = new S3Aws(s3Params, s3Params.getRegion());
        AmazonS3 s3Client = s3Aws.getClient();

        S3Object s3object = s3Client.getObject(s3Params.getBucket(), s3Params.getKey());
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        FileUtils.copyInputStreamToFile(inputStream, new File(pathname));
    }
}
