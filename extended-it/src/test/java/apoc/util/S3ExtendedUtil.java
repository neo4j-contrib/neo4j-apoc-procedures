package apoc.util;

public class S3ExtendedUtil {
    public static String removeRegionFromUrl(S3ExtendedContainer s3ExtendedContainer, String url) {
        return url.replace(s3ExtendedContainer.getEndpointConfiguration().getSigningRegion() + ".", "");
    }

    public static String putToS3AndGetUrl(S3ExtendedContainer s3ExtendedContainer, String filename) {
        String url = s3ExtendedContainer.putFile(filename);
        return removeRegionFromUrl(s3ExtendedContainer, url);
    }
}
