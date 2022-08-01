package apoc.util.s3;

import com.amazonaws.services.s3.AmazonS3;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

public class S3UploadUtils {

    private S3UploadUtils() {}

    public static OutputStream writeFile(String s3Url) throws IOException {
        S3Params s3Params = S3ParamsExtractor.extract(new URL(s3Url));
        S3Aws s3Aws = new S3Aws(s3Params, s3Params.getRegion());
        AmazonS3 s3 = s3Aws.getClient();
        return new S3OutputStream(s3, s3Params.getBucket(), s3Params.getKey());
    }
}
