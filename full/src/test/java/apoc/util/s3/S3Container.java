/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.util.s3;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import apoc.util.Util;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.File;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

public class S3Container implements AutoCloseable {
    private static final String S3_BUCKET_NAME = "test-bucket";
    private final LocalStackContainer localstack;
    private final AmazonS3 s3;

    public S3Container() {
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
        return localstack.getEndpointConfiguration(S3);
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

    public String putFile(String fileName) {
        final File file = new File(fileName);
        s3.putObject(S3_BUCKET_NAME, file.getName(), file);
        return getUrl(file.getName());
    }
}
