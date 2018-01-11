package apoc.load;

import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;

public class MinioSetUp {

    private static final String S3_PROTOCOL = "s3://";
    private static final String ACCESS_KEY = "Q3AM3UQ867SPQQA43P2F";
    private static final String SECRET_KEY = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG";
    private static final String ENDPOINT = "play.minio.io:9000";
    private static final String REGION = "us-east-1";

    private MinioClient minioClient;
    private String bucketName;

    public MinioSetUp(String bucketName) throws Exception{
        minioClient = new MinioClient("https://" + ENDPOINT, ACCESS_KEY, SECRET_KEY, REGION);
        this.bucketName = bucketName;
    }

    public String putFile(String filePath) throws Exception{
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        if(!minioClient.bucketExists(bucketName)) {
            minioClient.makeBucket(bucketName);
        }
        minioClient.putObject(bucketName,fileName, filePath);

        return S3_PROTOCOL + ENDPOINT + "/" + bucketName +  "/" + fileName + "?accessKey=" + ACCESS_KEY + "&secretKey=" + SECRET_KEY;
    }

    public void deleteAll() throws Exception{
        Iterable<Result<Item>> results = minioClient.listObjects(bucketName);
        for (Result<Item> result : results) {
            minioClient.removeObject(bucketName, result.get().objectName());
        }
        if (minioClient.bucketExists(bucketName)) {
            minioClient.removeBucket(bucketName);
        }
    }
}
