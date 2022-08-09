package apoc.util.s3;

public class S3Params {

    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final String endpoint;
    private final String bucket;
    private final String key;
    private final String region;

    public S3Params(String accessKey, String secretKey, String sessionToken,
                    String endpoint, String bucket, String key, String region) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.sessionToken = sessionToken;
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.key = key;
        this.region = region;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getBucket() { return bucket; }

    public String getKey() {
        return key;
    }

    public String getRegion() {
        return region;
    }

}
