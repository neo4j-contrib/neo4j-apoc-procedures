package apoc.util.s3;

import apoc.util.StreamConnection;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class S3Aws {

    AmazonS3 s3Client;
    private static final List<String> LOCALSTACK_IPS = Arrays.asList("127.0.0.1", "0.0.0.0", "localhost");

    public S3Aws(S3Params s3Params, String region) {

        AWSCredentialsProvider credentialsProvider = getCredentialsProvider(
                s3Params.getAccessKey(), s3Params.getSecretKey(), s3Params.getSessionToken());

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(credentialsProvider);

        // If it is a localstack connection, use HTTP instead of HTTPS.
        if (StringUtils.isNotBlank(s3Params.getEndpoint()) &&
                LOCALSTACK_IPS.stream().anyMatch(LOCALSTACK_IPS::contains)) {
            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.HTTP);
            builder.withClientConfiguration(config);
        } else {
            builder.withClientConfiguration(S3URLConnection.buildClientConfig());
        }

        builder.withPathStyleAccessEnabled(true);

        region = Objects.nonNull(region) ? region : s3Params.getRegion();
        String endpoint = s3Params.getEndpoint();
        if (Objects.nonNull(endpoint)) {
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3Params.getEndpoint(), region));
        } else if (Objects.nonNull(region)) {
            builder.withRegion(region);
        }

        s3Client = builder.build();
    }

    public AmazonS3 getClient() {
        return s3Client;
    }

    public StreamConnection getS3AwsInputStream(S3Params s3Params) {

        S3Object s3Object = s3Client.getObject(s3Params.getBucket(), s3Params.getKey());
        ObjectMetadata metadata = s3Object.getObjectMetadata();
        return new StreamConnection() {
            @Override
            public InputStream getInputStream() {
                return s3Object.getObjectContent();
            }

            @Override
            public String getEncoding() {
                return metadata.getContentEncoding();
            }

            @Override
            public long getLength() {
                return metadata.getContentLength();
            }

            @Override
            public String getName() {
                return s3Params.getKey();
            }
        };
    }

    private static AWSCredentialsProvider getCredentialsProvider(
            final String accessKey, final String secretKey, final String sessionToken) {

        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            final AWSCredentials credentials;
            if (StringUtils.isNotBlank(sessionToken)) {
                credentials = new BasicAWSCredentials(accessKey, secretKey);
            } else {
                credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
            }
            return new AWSStaticCredentialsProvider(credentials);
        }
        return new DefaultAWSCredentialsProviderChain();
    }
}
