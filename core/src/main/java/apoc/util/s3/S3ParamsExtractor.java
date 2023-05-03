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

import apoc.util.Util;
import com.amazonaws.regions.Regions;

import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

public class S3ParamsExtractor {

    private static final String PROTOCOL = "s3";
    private static final String ACCESS_KEY = "accessKey";
    private static final String SECRET_KEY = "secretKey";
    private static final String SESSION_TOKEN = "sessionToken";

    public static S3Params extract(URL url) throws IllegalArgumentException {
        return extract(url.toString());
    }

    public static S3Params extract(String url) throws IllegalArgumentException {

        URI uri = URI.create(url);

        if (!PROTOCOL.equals(uri.getScheme())) {
            throw new IllegalArgumentException("Unsupported protocol '" + uri.getScheme() + "'");
        }

        //aws credentials
        String accessKey = null;
        String secretKey = null;
        String sessionToken = null;

        if (uri.getUserInfo() != null) {
            String[] credentials = uri.getUserInfo().split(":");
            if (credentials.length > 1) {
                accessKey = credentials[0];
                secretKey = credentials[1];
            }
            if (credentials.length > 2) {
                sessionToken = credentials[2];
            }
            // User info part cannot contain session token.
        } else {
            Map<String, String> params = Util.getRequestParameter(uri.getQuery());
            if(Objects.nonNull(params)) {
                if(params.containsKey(ACCESS_KEY)){accessKey = params.get(ACCESS_KEY);}
                if(params.containsKey(SECRET_KEY)){secretKey = params.get(SECRET_KEY);}
                if(params.containsKey(SESSION_TOKEN)){sessionToken = params.get(SESSION_TOKEN);}
            }
        }

        // We have to use the getAuthority here instead of getHost, because addresses
        // like us-east-1.127.0.0.1:55220 would return null for the later one.
        // The downside is we have to clean the credentials preceding the @ if they are there,
        // which .getHost would not return
        String endpoint = uri.getAuthority();
        int atIndex = endpoint.indexOf( "@" );
        if (atIndex != -1)
            endpoint = endpoint.substring( atIndex + 1 );

        Integer slashIndex = uri.getPath().indexOf("/", 1);
        String key;
        String bucket ;

        if(slashIndex > 0){
            // key
            key = uri.getPath().substring(slashIndex + 1);
            // bucket
            bucket = uri.getPath().substring(1, slashIndex);
        }
        else{
            throw new IllegalArgumentException("Invalid url. Must be:\n's3://accessKey:secretKey@endpoint:port/bucket/key' or\n's3://endpoint:port/bucket/key?accessKey=accessKey&secretKey=secretKey'");
        }


        String region = null;

        if (Objects.nonNull(endpoint)) {

            // Look for endpoint contains region
            for (Regions r: Regions.values()){
                if(endpoint.toLowerCase().contains(r.getName().toLowerCase())){
                    region = r.getName().toLowerCase();
                    break;
                }
            }

            if(Objects.nonNull(region)) {
                //has specific endpoints for regions, otherwise remove region from endpoint
                if(!endpoint.contains("amazonaws.com")) {
                    endpoint = endpoint.substring(endpoint.indexOf(".") + 1);
                }

                // If it contains region only, it is an invalid endpoint.
                if (region.toLowerCase().equals(endpoint.toLowerCase())) {
                    endpoint = "";
                }
            }
        }

        if (endpoint != null) {
            endpoint = endpoint.replaceAll( ":443", "").replaceAll( ":80", "" );
        }

        if (Objects.nonNull(endpoint) && endpoint.isEmpty()) {
            endpoint = null;
        }

        if (endpoint != null && "true".equals(System.getProperty("com.amazonaws.sdk.disableCertChecking", "false"))) {
            endpoint = "http://" + endpoint;
        }

        return new S3Params(accessKey, secretKey, sessionToken, endpoint, bucket, key, region);
    }
}
