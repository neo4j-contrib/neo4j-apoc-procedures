package apoc.azure;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.UUID;

public class AzureStorageBaseTest {

    public static GenericContainer<?> azuriteContainer;
    public static BlobContainerClient containerClient;
    
    @BeforeClass
    public static void setUp() throws Exception {
        DockerImageName azuriteImg = DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite");
        azuriteContainer = new GenericContainer<>(azuriteImg)
                .withExposedPorts(10000);

        azuriteContainer.start();

        var accountName = "devstoreaccount1";
        var accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        var blobEndpoint = "http://%s:%d/%s".formatted(azuriteContainer.getHost(), azuriteContainer.getMappedPort(10000), accountName);
        var connectionString = "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;"
                .formatted(accountName, accountKey, blobEndpoint);

        containerClient = new BlobContainerClientBuilder()
                .connectionString(connectionString)
                .containerName("test-container")
                .buildClient();
        containerClient.create();
    }    
    
    @AfterClass
    public static void teardown() {
        azuriteContainer.close();
    }

    public static String putToAzureStorageAndGetUrl(String url) {
        try {
            File file = new File(url);
            byte[] content = FileUtils.readFileToByteArray(file);

            var blobClient = getBlobClient(content);
            BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);
            OffsetDateTime expiryTime = OffsetDateTime.now().plusHours(1);
            String sasToken = blobClient.generateSas(new BlobServiceSasSignatureValues(expiryTime, permission), new Context("Azure-Storage-Log-String-To-Sign", "true"));
            return blobClient.getBlobUrl() + "?" + sasToken;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static BlobClient getBlobClient(byte[] content) {
        var blobName = "blob-" + UUID.randomUUID();
        var blobClient = containerClient.getBlobClient(blobName);
        blobClient.upload(new ByteArrayInputStream(content));
        return blobClient;
    }

}