package apoc.s3;

import apoc.util.s3.S3Container;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class S3BaseTest {
    protected static S3Container s3Container;

    @BeforeClass
    public static void baseBeforeClass() {
        s3Container = new S3Container();

        // Used by S3ParamsExtractor to prepend http:// to endpoints for local testing
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty("com.amazonaws.sdk.disableCertChecking");

        s3Container.close();
    }
}
