package apoc.util.s3;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.FileUtils;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

public class S3Container implements AutoCloseable {
    private final static String S3_BUCKET_NAME = "test-bucket";
    private LocalStackContainer localstack;
    private final AmazonS3 s3;


    public S3Container() {
        TestUtil.ignoreException(() -> {
            localstack = new LocalStackContainer("0.8.10")
                    .withServices(S3);
            localstack.start();
        }, Exception.class);
        assumeNotNull(localstack);
        assumeTrue(localstack.isRunning());
        s3 = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(getEndpointConfiguration())
                .withCredentials(getCredentialsProvider())
                .build();
        s3.createBucket(getBucket());
    }

    public void close() {
        Util.close(localstack);
    }

    public AwsClientBuilder.EndpointConfiguration getEndpointConfiguration() {
        return localstack.getEndpointConfiguration(S3);
    }

    public AWSCredentialsProvider getCredentialsProvider() {
        return localstack.getDefaultCredentialsProvider();
    }

    public String getBucket() {
        return S3_BUCKET_NAME;
    }

    public String getUrl(String key) {
        return String.format("s3://%s.%s/%s/%s?accessKey=%s&secretKey=%s",
                localstack.getEndpointConfiguration(S3).getSigningRegion(),
                localstack.getEndpointConfiguration(S3).getServiceEndpoint()
                        .replace("http://", ""),
                S3_BUCKET_NAME,
                key,
                localstack.getDefaultCredentialsProvider().getCredentials().getAWSAccessKeyId(),
                localstack.getDefaultCredentialsProvider().getCredentials().getAWSSecretKey());
    }

    public String putFile(String fileName) {
        final File file = new File(fileName);
        s3.putObject(getBucket(), file.getName(), file);
        return getUrl(file.getName());
    }

    public void verifyUpload(File directory, String fileName, String expected) throws IOException {
        String s3Url = getUrl(fileName);
        S3Container.readFile(s3Url, Paths.get(directory.toString(), fileName).toString());
        assertEquals(expected, readFileToString(directory.getAbsolutePath(), fileName));
    }

    public static void verifyUpload(File directoryExpected, File directory, String s3Url, String fileName) throws IOException {
        readFile(s3Url, Paths.get(directory.toString(), fileName).toString());
        assertFileEquals(directoryExpected, fileName);
    }

    public static void assertFileEquals(File directoryExpected, String fileName) {
        String actualText = TestUtil.readFileToString(new File(directoryExpected, fileName));
        assertStreamEquals(directoryExpected, fileName, actualText);
    }

    public static void assertStreamEquals(File directoryExpected, String fileName, String actualText) {
        String expectedText = TestUtil.readFileToString(new File(directoryExpected, fileName));
        String[] actualArray = actualText.split("\n");
        String[] expectArray = expectedText.split("\n");
        assertEquals(expectArray.length, actualArray.length);
        for (int i = 0; i < actualArray.length; i++) {
            assertEquals(JsonUtil.parse(expectArray[i],null, Object.class), JsonUtil.parse(actualArray[i],null, Object.class));
        }
    }

    public static void readFile(String s3Url, String pathname) throws IOException {
        S3Params s3Params = S3ParamsExtractor.extract(new URL(s3Url));
        S3Aws s3Aws = new S3Aws(s3Params, s3Params.getRegion());
        AmazonS3 s3Client = s3Aws.getClient();

        S3Object s3object = s3Client.getObject(s3Params.getBucket(), s3Params.getKey());
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        FileUtils.copyInputStreamToFile(inputStream, new File(pathname));
    }

    private static String readFileToString(String directory, String fileName) {
        return TestUtil.readFileToString(new File(directory, fileName));
    }


}
