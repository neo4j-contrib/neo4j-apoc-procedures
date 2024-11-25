package apoc.util;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import static java.net.HttpURLConnection.HTTP_OK;

import com.google.cloud.storage.Bucket;

public class GoogleCloudStorageContainerExtension extends GenericContainer<GoogleCloudStorageContainerExtension> {

    public GoogleCloudStorageContainerExtension() {
        super("fsouza/fake-gcs-server:latest");
        this.withCommand("-scheme http");

        setWaitStrategy(new HttpWaitStrategy()
                .forPath("/storage/v1/b")
                .forPort(4443)
                .forStatusCodeMatching(response -> response == HTTP_OK));

//        addExposedPort(4443);
        addFixedExposedPort(4443, 4443);
    }

    public GoogleCloudStorageContainerExtension withMountedResourceFile(String resourceFilePath, String gcsPath) {
        this.withClasspathResourceMapping(resourceFilePath, "/data" + gcsPath, BindMode.READ_ONLY);
        return this;
    }

    public static String gcsUrl(GoogleCloudStorageContainerExtension gcs, String file) {
        String path = "b/folder/o/%s?alt=media".formatted(file);
        return String.format("http://%s:%d/storage/v1/%s", gcs.getContainerIpAddress(), gcs.getMappedPort(4443), path);
    }

    public String gcsUrl(String bucketName, String objectName) {
        // createBucket(bucketName);
        return String.format("gs://%s/%s", bucketName, objectName);
    }

    public void createBucket(String bucketName) {
        
        Storage storage = StorageOptions.newBuilder()
                .setHost("http://localhost:" + getMappedPort(4443) ) // Connect to the fake GCS server
                .setProjectId("test-project")
                .build()
                .getService();

        Bucket bucket = storage.create(Bucket.newBuilder(bucketName).build());
        System.out.println("Created bucket: " + bucket.getName());
    }
}
