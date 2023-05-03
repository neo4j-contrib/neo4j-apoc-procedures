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
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.regions.Regions;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Objects;

public class S3URLConnection extends URLConnection {

    public static final String PROP_S3_HANDLER_USER_AGENT = "s3.handler.userAgent";
    public static final String PROP_S3_HANDLER_PROTOCOL = "s3.handler.protocol";
    public static final String PROP_S3_HANDLER_SIGNER_OVERRIDE = "s3.handler.signerOverride";

    public S3URLConnection(URL url) {
        super(url);
    }

    @Override
    public void connect() {
    }

    public static ClientConfiguration buildClientConfig() {
        final String userAgent = System.getProperty(PROP_S3_HANDLER_USER_AGENT, null);
        final String protocol = System.getProperty(PROP_S3_HANDLER_PROTOCOL, "https").toLowerCase();
        final String signerOverride = System.getProperty(PROP_S3_HANDLER_SIGNER_OVERRIDE, null);

        final ClientConfiguration clientConfig = new ClientConfiguration()
                .withProtocol("https".equals(protocol) ? Protocol.HTTPS : Protocol.HTTP);

        if (userAgent != null) {
            clientConfig.setUserAgentPrefix(userAgent);
        }
        if (signerOverride != null) {
            clientConfig.setSignerOverride(signerOverride);
        }

        return clientConfig;
    }


    public static StreamConnection openS3InputStream(URL url) throws IOException {
        S3Params s3Params = S3ParamsExtractor.extract(url);
        String region = Objects.nonNull(s3Params.getRegion()) ? s3Params.getRegion() : Regions.US_EAST_1.getName();
        return new S3Aws(s3Params, region).getS3AwsInputStream(s3Params);
    }

}
