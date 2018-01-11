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

    public static S3Params extract(URL url) throws  IllegalArgumentException {

        if (!PROTOCOL.equals(url.getProtocol())) {
            throw new IllegalArgumentException("Unsupported protocol '" + url.getProtocol() + "'");
        }

        //aws credentials
        String accessKey = null;
        String secretKey = null;

        if (url.getUserInfo() != null) {
            String[] credentials = url.getUserInfo().split(":");
            accessKey = credentials[0];
            secretKey = credentials[1];
        } else {
            Map<String, String> params = Util.getRequestParameter(url.getQuery());
            if(params.containsKey(ACCESS_KEY)){accessKey = params.get(ACCESS_KEY);}
            if(params.containsKey(SECRET_KEY)){secretKey = params.get(SECRET_KEY);}
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

        //look for endpoint contains region.
        for (Regions r: Regions.values()){
            if(endpoint.toLowerCase().contains(r.getName().toLowerCase())){
                region = r.getName().toLowerCase();
            }
        }

        //has specific endpoints for regions otherwise remove region from endpoint
        if(Objects.nonNull(region) && !endpoint.contains("amazonaws.com")) {
            endpoint = endpoint.substring(endpoint.indexOf(".") + 1);
        }

        if (url.getPort() != 80 && url.getPort() != 443 && url.getPort() > 0) {
            endpoint += ":" + url.getPort();
        }

        return new S3Params(accessKey, secretKey, endpoint, bucket, key, region);
    }
}
