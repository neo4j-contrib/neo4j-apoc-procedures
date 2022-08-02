package apoc.util;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.time.Duration;

import static java.net.HttpURLConnection.HTTP_OK;

public class GoogleCloudStorageContainerExtension extends GenericContainer<GoogleCloudStorageContainerExtension> {

    public GoogleCloudStorageContainerExtension() {
        super("fsouza/fake-gcs-server:latest");
        this.withCommand("-scheme http");

        setWaitStrategy(new HttpWaitStrategy()
                .forPath("/storage/v1/b")
                .forPort(4443)
                .forStatusCodeMatching(response -> response == HTTP_OK));

        addExposedPort(4443);
    }

    public GoogleCloudStorageContainerExtension withMountedResourceFile(String resourceFilePath, String gcsPath) {
        this.withClasspathResourceMapping(resourceFilePath, "/data" + gcsPath, BindMode.READ_ONLY);
        return this;
    }
}
