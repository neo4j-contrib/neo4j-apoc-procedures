package apoc.util.s3;

import apoc.util.Util;
import com.amazonaws.regions.Regions;

import java.net.URL;
import java.util.Map;
import java.util.Objects;

public class S3ParamsExtractor {

    private static final String PROTOCOL = "s3";
    private static final String ACCESS_KEY = "accessKey";
    private static final String SECRET_KEY = "secretKey";
    private static final String SESSION_TOKEN = "sessionToken";

    public static S3Params extract(URL url) throws  IllegalArgumentException {

        if (!PROTOCOL.equals(url.getProtocol())) {
            throw new IllegalArgumentException("Unsupported protocol '" + url.getProtocol() + "'");
        }

        //aws credentials
        String accessKey = null;
        String secretKey = null;
        String sessionToken = null;

        if (url.getUserInfo() != null) {
            String[] credentials = url.getUserInfo().split(":");
            if (credentials.length > 1) {
                accessKey = credentials[0];
                secretKey = credentials[1];
            }
            if (credentials.length > 2) {
                sessionToken = credentials[2];
            }
            // User info part cannot contain session token.
        } else {
            Map<String, String> params = Util.getRequestParameter(url.getQuery());
            if(Objects.nonNull(params)) {
                if(params.containsKey(ACCESS_KEY)){accessKey = params.get(ACCESS_KEY);}
                if(params.containsKey(SECRET_KEY)){secretKey = params.get(SECRET_KEY);}
                if(params.containsKey(SESSION_TOKEN)){sessionToken = params.get(SESSION_TOKEN);}
            }
        }

        // endpoint
        String endpoint = url.getHost();

        Integer slashIndex = url.getPath().lastIndexOf("/");
        String key;
        String bucket ;

        if(slashIndex > 0){
            // key
            key = url.getPath().substring(slashIndex + 1);
            // bucket
            bucket = url.getPath().substring(1, slashIndex);
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

        if (url.getPort() != 80 && url.getPort() != 443 && url.getPort() > 0) {
            endpoint += ":" + url.getPort();
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
