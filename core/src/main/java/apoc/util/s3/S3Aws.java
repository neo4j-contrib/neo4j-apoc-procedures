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

import apoc.util.StreamConnection;
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

import java.io.InputStream;
import java.util.Objects;

public class S3Aws {

    AmazonS3 s3Client;

    public S3Aws(S3Params s3Params, String region) {

        AWSCredentialsProvider credentialsProvider = getCredentialsProvider(
                s3Params.getAccessKey(), s3Params.getSecretKey(), s3Params.getSessionToken());

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(credentialsProvider)
                .withClientConfiguration(S3URLConnection.buildClientConfig())
                .withPathStyleAccessEnabled(true);

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

        if (Objects.nonNull(accessKey) && !accessKey.isEmpty()
                && Objects.nonNull(secretKey) && !secretKey.isEmpty()) {
            final AWSCredentials credentials;
            if (Objects.isNull(sessionToken) || sessionToken.isEmpty()) {
                credentials = new BasicAWSCredentials(accessKey, secretKey);
            } else {
                credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
            }
            return new AWSStaticCredentialsProvider(credentials);
        }
        return new DefaultAWSCredentialsProviderChain();
    }
}
