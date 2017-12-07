package apoc.util.s3util;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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

    public InputStream getS3AwsInputStream(S3Params s3Params){

        return s3Client.getObject(s3Params.getBucket(), s3Params.getKey()).getObjectContent();
    }
}
