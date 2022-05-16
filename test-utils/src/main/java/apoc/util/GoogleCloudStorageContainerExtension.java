package apoc.util;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

public class GoogleCloudStorageContainerExtension extends GenericContainer<GoogleCloudStorageContainerExtension> {

    public GoogleCloudStorageContainerExtension() {
        super("fsouza/fake-gcs-server:latest");
        this.addFixedExposedPort(4443, 4443);
        this.withCommand("-scheme http");
    }

    public GoogleCloudStorageContainerExtension withMountedResourceFile(String resourceFilePath, String gcsPath) {
        this.withClasspathResourceMapping(resourceFilePath, "/data" + gcsPath, BindMode.READ_ONLY);
        return this;
    }
}
