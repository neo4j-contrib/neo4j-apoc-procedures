package apoc.util.s3;

import apoc.util.StreamConnection;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;

public class S3Aws {

    AmazonS3 s3Client;

    public S3Aws(S3Params s3Params, String region){
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3Params.getAccessKey(), s3Params.getSecretKey())))
                .withClientConfiguration(S3URLConnection.buildClientConfig())
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3Params.getEndpoint(), region))
                .build();
    }

    public StreamConnection getS3AwsInputStream(S3Params s3Params){

        S3Object s3Object = s3Client.getObject(s3Params.getBucket(), s3Params.getKey());
        ObjectMetadata metadata = s3Object.getObjectMetadata();
        return new StreamConnection() {
            @Override
            public InputStream getInputStream() throws IOException {
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
        };
    }
}
