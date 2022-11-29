package apoc.util.s3;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class S3BaseTest {
    protected static S3Container s3Container;

    @BeforeClass
    public static void baseBeforeClass() {
        s3Container = new S3Container();

        // In test environment we skip the MD5 validation that can cause issues
        System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation");
        System.clearProperty("com.amazonaws.sdk.disableCertChecking");

        s3Container.close();
    }
}
