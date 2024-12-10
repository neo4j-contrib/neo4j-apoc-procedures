package apoc.util.s3;

public class S3Util {
    public static String removeRegionFromUrl(S3Container s3Container, String url) {
        return url.replace(s3Container.getEndpointConfiguration().getSigningRegion() + ".", "");
    }

    public static String putToS3AndGetUrl(S3Container s3Container, String filename) {
        String url = s3Container.putFile(filename);
        return removeRegionFromUrl(s3Container, url);
    }
}
