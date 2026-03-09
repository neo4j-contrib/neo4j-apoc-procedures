package apoc.util.s3;

import java.net.URI;

public class S3Util {
    public static String removeRegionFromUrl(S3Container s3Container, String url) {
        URI endpoint = s3Container.getEndpoint();
        System.out.println("endpoint = " + endpoint);
        System.out.println("endpoint.getPath() = " + endpoint.getPath());
        return url.replace(endpoint + ".", "");
    }

    public static String putToS3AndGetUrl(S3Container s3Container, String filename) {
        String url = s3Container.putFile(filename);
        System.out.println("url = " + url);
        return removeRegionFromUrl(s3Container, url);
    }
}
