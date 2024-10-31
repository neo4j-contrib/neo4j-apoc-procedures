package apoc.util;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import org.apache.commons.lang3.tuple.Pair;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

public class S3ExtendedContainer implements AutoCloseable {
    private static final String S3_BUCKET_NAME = "test-bucket";
    private final LocalStackContainer localstack;
    private final AmazonS3 s3;

    public S3ExtendedContainer() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.2.0")).withServices(S3);
        localstack.addExposedPorts(4566);
        localstack.start();

        s3 = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(getEndpointConfiguration())
                .withCredentials(getCredentialsProvider())
                .build();
        s3.createBucket(S3_BUCKET_NAME);
    }

    public void close() {
        Util.close(localstack);
    }

    public AwsClientBuilder.EndpointConfiguration getEndpointConfiguration() {
        return new AwsClientBuilder.EndpointConfiguration(
                localstack.getEndpoint().toString(), localstack.getRegion());
    }

    public AWSCredentialsProvider getCredentialsProvider() {
        return new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey()));
    }

    public String getUrl(String key) {
        return String.format(
                "s3://%s.%s/%s/%s?accessKey=%s&secretKey=%s",
                getEndpointConfiguration().getSigningRegion(),
                getEndpointConfiguration().getServiceEndpoint().replace("http://", ""),
                S3_BUCKET_NAME,
                key,
                getCredentialsProvider().getCredentials().getAWSAccessKeyId(),
                getCredentialsProvider().getCredentials().getAWSSecretKey());
    }

    @SuppressWarnings("unused") // used from extended
    public String putFile(String fileName) {
        final File file = new File(fileName);
        s3.putObject(S3_BUCKET_NAME, file.getName(), file);
        return getUrl(file.getName());
    }

    public List<S3ObjectSummary> listBucket(String bucketName, String prefix) {
        ObjectListing listing = s3.listObjects( bucketName, prefix );
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        while (listing.isTruncated()) {
            listing = s3.listNextBatchOfObjects (listing);
            summaries.addAll (listing.getObjectSummaries());
        }

        return summaries;
    }

    public void putObjectToS3(String key, byte[] data) {
        InputStream is = new ByteArrayInputStream(data);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(data.length);

        PutObjectRequest putObjectRequest = new PutObjectRequest(S3_BUCKET_NAME, key, is, metadata);

        try {
            s3.putObject(putObjectRequest);
        } catch (Exception e) {
            System.out.println("Error Message:    " + e.getMessage());
        }
    }

    public Pair<String, String> parseS3URI(String url) {
        try {
            var uri = new URI(url);

            if ("s3".equals(uri.getScheme())) {
                var bucket = uri.getPath().split("/")[1];
                var key = uri.getPath().split("/")[2];

                return Pair.of(bucket, key);
            }

            return null;
        }
        catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }
}

