package apoc.util.s3;

import org.junit.Test;
import static org.junit.Assert.*;

public class S3ParamsExtractorTest {

    @Test
    public void testEncodedS3Url() throws Exception {
        S3Params params = S3ParamsExtractor.extract("s3://accessKeyId:some%2Fsecret%2Fkey:some%2Fsession%2Ftoken@s3.us-east-2.amazonaws.com:1234/bucket/path/to/key");
        assertEquals("some/secret/key", params.getSecretKey());
        assertEquals("some/session/token", params.getSessionToken());
        assertEquals("accessKeyId", params.getAccessKey());
        assertEquals("bucket", params.getBucket());
        assertEquals("path/to/key", params.getKey());
        assertEquals("s3.us-east-2.amazonaws.com:1234", params.getEndpoint());
        assertEquals("us-east-2", params.getRegion());
    }

    @Test
    public void testEncodedS3UrlQueryParams() throws Exception {
        S3Params params = S3ParamsExtractor.extract("s3://s3.us-east-2.amazonaws.com:1234/bucket/path/to/key?accessKey=accessKeyId&secretKey=some%2Fsecret%2Fkey&sessionToken=some%2Fsession%2Ftoken");
        assertEquals("some/secret/key", params.getSecretKey());
        assertEquals("some/session/token", params.getSessionToken());
        assertEquals("accessKeyId", params.getAccessKey());
        assertEquals("bucket", params.getBucket());
        assertEquals("path/to/key", params.getKey());
        assertEquals("s3.us-east-2.amazonaws.com:1234", params.getEndpoint());
    }

    @Test
    public void testExtractEndpointPort() throws Exception {
        assertEquals("s3.amazonaws.com", S3ParamsExtractor.extract("s3://s3.amazonaws.com:80/bucket/path/to/key").getEndpoint());
        assertEquals("s3.amazonaws.com:1234", S3ParamsExtractor.extract("s3://s3.amazonaws.com:1234/bucket/path/to/key").getEndpoint());
    }

    @Test
    public void testExtractRegion() throws Exception {
        assertEquals("us-east-2", S3ParamsExtractor.extract("s3://s3.us-east-2.amazonaws.com:80/bucket/path/to/key").getRegion());
        assertNull(S3ParamsExtractor.extract("s3://s3.amazonaws.com:80/bucket/path/to/key").getRegion());
    }
}
