package apoc.couchbase;

import apoc.util.TestUtil;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.testcontainers.couchbase.CouchbaseContainer;

import static apoc.couchbase.CouchbaseTestUtils.*;
import static apoc.util.TestUtil.isTravis;
import static org.junit.Assume.*;

/**
 * Created by alberto.delazzari on 23/08/2018.
 */
@Ignore // The same tests are covered from CouchbaseIT, for now we disable this in order to reduce the build time
public class CouchbaseManagerIT {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    private static final String COUCHBASE_CONFIG_KEY = "demo";

    private static final String BUCKET_NAME_WITH_PASSWORD = "mysecretbucket";

    private static final String BUCKET_PASSWORD = "drowssap";

    private static int COUCHBASE_SERVER_VERSION;

    private static GraphDatabaseService graphDB;

    public static CouchbaseContainer couchbase;


    @BeforeClass
    public static void setUp() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            couchbase = new CouchbaseContainer()
                    .withClusterAdmin(USERNAME, PASSWORD)
                    .withNewBucket(DefaultBucketSettings.builder()
                            .password(PASSWORD)
                            .name(BUCKET_NAME)
                            .quota(100)
                            .type(BucketType.COUCHBASE)
                            .build());
            couchbase.start();
        }, Exception.class);
        assumeNotNull(couchbase);
        assumeTrue("couchbase must be running", couchbase.isRunning());
        boolean isFilled = fillDB(couchbase.getCouchbaseCluster());
        assumeTrue("should fill Couchbase with data", isFilled);
        COUCHBASE_SERVER_VERSION = getVersion(couchbase);

        String baseConfigKey = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COUCHBASE_CONFIG_KEY + ".";

        graphDB = TestUtil.apocGraphDatabaseBuilder()
                .setConfig(baseConfigKey + CouchbaseManager.URI_CONFIG_KEY, "localhost")
                .setConfig(baseConfigKey + CouchbaseManager.USERNAME_CONFIG_KEY, USERNAME)
                .setConfig(baseConfigKey + CouchbaseManager.PASSWORD_CONFIG_KEY, PASSWORD)
                .setConfig(baseConfigKey + CouchbaseManager.PORT_CONFIG_KEY, couchbase.getMappedPort(8091).toString())
                .setConfig("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + CONNECTION_TIMEOUT_CONFIG_KEY,
                        CONNECTION_TIMEOUT_CONFIG_VALUE)
                .setConfig("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + SOCKET_CONNECT_TIMEOUT_CONFIG_KEY,
                        SOCKET_CONNECT_TIMEOUT_CONFIG_VALUE)
                .setConfig("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + KV_TIMEOUT_CONFIG_KEY,
                        KV_TIMEOUT_CONFIG_VALUE)
                .newGraphDatabase();
    }

    @AfterClass
    public static void tearDown() {
        if (couchbase != null) {
            couchbase.stop();
            graphDB.shutdown();
        }
    }

    /**
     * This test should pass regardless the Couchbase Server version (it should pass both on 4.x and 5.x)
     */
    @Test
    public void testGetConnectionWithKey() {
        try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(COUCHBASE_CONFIG_KEY, BUCKET_NAME);) {
            Assert.assertTrue(couchbaseConnection.get("artist:vincent_van_gogh").content().containsKey("notableWorks"));
        }
    }

    /**
     * This test will be ignored for Couchbase Server 5.x
     * We are testing the access to a bucket with a password
     */
    @Test
    public void testGetConnectionWithKeyAndBucketPassword() {
        assumeTrue(COUCHBASE_SERVER_VERSION == 4);
        try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(COUCHBASE_CONFIG_KEY, BUCKET_NAME_WITH_PASSWORD + ":" + BUCKET_PASSWORD);) {
            Assert.assertTrue(couchbaseConnection.get("artist:vincent_van_gogh").content().containsKey("notableWorks"));
        }
    }

    /**
     * This test will be ignored for Couchbase Server 5.x
     * We are testing the access to a bucket with a password without passing the password
     * It should raise a {@link ConfigurationException} with the following message "Could not open bucket."
     */
    @Test
    public void testGetConnectionWithKeyAndBucketPasswordFailsWithNoPassword() {
        assumeTrue(COUCHBASE_SERVER_VERSION == 4);
        try (CouchbaseConnection conn = CouchbaseManager.getConnection(COUCHBASE_CONFIG_KEY, BUCKET_NAME_WITH_PASSWORD /*Only bucket name and no bucket password*/);) {
            exceptionRule.expect(ConfigurationException.class);
            exceptionRule.expectMessage("Could not open bucket.");
        }
    }

    @Test
    public void testGetConnectionWithHost() {
        try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection("couchbase://" + USERNAME + ":" + PASSWORD
                + "@localhost:" + couchbase.getMappedPort(8091), BUCKET_NAME);) {
            Assert.assertTrue(couchbaseConnection.get("artist:vincent_van_gogh").content().containsKey("notableWorks"));
        }
    }
}
