package apoc.s3;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class S3BaseTest {
    protected static ExtendedS3Container s3Container;

    @BeforeClass
    public static void baseBeforeClass() {
        s3Container = new ExtendedS3Container();

        // In test environment we skip the MD5 validation that can cause issues
        System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");

        // Used by S3ParamsExtractor to prepend http:// to endpoints for local testing
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");

        // Disable checksum validation
        System.setProperty("software.amazon.awssdk.sdk.disableBinaryChecksumValidation", "true");
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty("com.amazonaws.sdk.disableCertChecking");

        s3Container.close();
    }
}
