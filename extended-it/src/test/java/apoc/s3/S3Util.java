package apoc.s3;

import java.net.URI;

public class S3Util {
    public static String removeRegionFromUrl(ExtendedS3Container s3Container, String url) {
        URI endpoint = s3Container.getEndpoint();
        return url.replace(endpoint + ".", "");
    }

    public static String putToS3AndGetUrl(ExtendedS3Container s3Container, String filename) {
        String url = s3Container.putFile(filename);
        return removeRegionFromUrl(s3Container, url);
    }
}
