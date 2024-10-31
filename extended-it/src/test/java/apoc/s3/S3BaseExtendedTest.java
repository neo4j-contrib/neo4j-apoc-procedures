package apoc.s3;

import apoc.util.S3ExtendedContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class S3BaseExtendedTest {
    protected static S3ExtendedContainer s3ExtendedContainer;

    @BeforeClass
    public static void baseBeforeClass() {
        s3ExtendedContainer = new S3ExtendedContainer();

        // In test environment we skip the MD5 validation that can cause issues
        System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation");
        System.clearProperty("com.amazonaws.sdk.disableCertChecking");

        s3ExtendedContainer.close();
    }
}
