package apoc.load.partial;

import apoc.util.s3.S3Aws;
import apoc.util.s3.S3Params;
import apoc.util.s3.S3ParamsExtractor;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * We put these methods in another class since they import `com.amazonaws.*` dependencies
 * which are extra-dependencies and if not included, will throw a NoClassDefFoundError
 */
public class LoadPartialAws {
    
    public static InputStream getS3ObjectInputStream(String path, int offset, int limit) throws IOException {
        S3Params s3Params = S3ParamsExtractor.extract(path);
        String region = Objects.nonNull(s3Params.getRegion()) ? s3Params.getRegion() : Regions.US_EAST_1.getName();
        S3Aws s3Aws = new S3Aws(s3Params, region);

        GetObjectRequest request = new GetObjectRequest(s3Params.getBucket(), s3Params.getKey());

        if (limit == 0) {
            request.withRange(offset);
        } else {
            request.withRange(offset, offset + limit - 1);
        }

        S3Object object = s3Aws.getClient().getObject(request);
        return s3Aws.getS3AwsInputStream(s3Params).getInputStream();// object.getObjectContent();
    }
}
