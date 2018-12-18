package apoc.couchbase;

import apoc.util.TestUtil;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assume.assumeTrue;

import static apoc.couchbase.CouchbaseTestUtils.*;

/**
 * Created by alberto.delazzari on 23/08/2018.
 */
public class CouchbaseManagerIT {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    private static final String COUCHBASE_CONFIG_KEY = "demo";

    private static final String BUCKET_NAME_WITH_PASSWORD = "mysecretbucket";

    private static final String BUCKET_PASSWORD = "drowssap";

    private static int COUCHBASE_SERVER_VERSION;

    @ClassRule
    public static CouchbaseContainer couchbase = new CouchbaseContainer()
            .withClusterAdmin(USERNAME, PASSWORD)
            .withNewBucket(DefaultBucketSettings.builder()
                    .password(PASSWORD)
                    .name(BUCKET_NAME)
                    .type(BucketType.COUCHBASE)
                    .build());


    @BeforeClass
    public static void setUp() throws Exception {
        boolean isFilled = fillDB(couchbase.getCouchbaseCluster());
        assumeTrue("should fill Couchbase with data", isFilled);
        COUCHBASE_SERVER_VERSION = getVersion(couchbase);

        String baseConfigKey = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COUCHBASE_CONFIG_KEY + ".";

        new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig(baseConfigKey + CouchbaseManager.URI_CONFIG_KEY, "localhost")
                .setConfig(baseConfigKey + CouchbaseManager.USERNAME_CONFIG_KEY, USERNAME)
                .setConfig(baseConfigKey + CouchbaseManager.PASSWORD_CONFIG_KEY, PASSWORD)
                .setConfig(baseConfigKey + CouchbaseManager.PORT_CONFIG_KEY, couchbase.getMappedPort(8091).toString())
                .newGraphDatabase();
    }

    /**
     * This test should pass regardless the Couchbase Server version (it should pass both on 4.x and 5.x)
     */
    @Test
    public void testGetConnectionWithKey() {
        CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(COUCHBASE_CONFIG_KEY, BUCKET_NAME);
        Assert.assertTrue(couchbaseConnection.get("artist:vincent_van_gogh").content().containsKey("notableWorks"));
    }

    /**
     * This test will be ignored for Couchbase Server 5.x
     * We are testing the access to a bucket with a password
     */
    @Test
    public void testGetConnectionWithKeyAndBucketPassword() {
        assumeTrue(COUCHBASE_SERVER_VERSION == 4);
        CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(COUCHBASE_CONFIG_KEY, BUCKET_NAME_WITH_PASSWORD + ":" + BUCKET_PASSWORD);
        Assert.assertTrue(couchbaseConnection.get("artist:vincent_van_gogh").content().containsKey("notableWorks"));
    }

    /**
     * This test will be ignored for Couchbase Server 5.x
     * We are testing the access to a bucket with a password without passing the password
     * It should raise a {@link ConfigurationException} with the following message "Could not open bucket."
     */
    @Test
    public void testGetConnectionWithKeyAndBucketPasswordFailsWithNoPassword() {
        assumeTrue(COUCHBASE_SERVER_VERSION == 4);
        exceptionRule.expect(ConfigurationException.class);
        exceptionRule.expectMessage("Could not open bucket.");
        CouchbaseManager.getConnection(COUCHBASE_CONFIG_KEY, BUCKET_NAME_WITH_PASSWORD /*Only bucket name and no bucket password*/);
    }

    @Test
    public void testGetConnectionWithHost() {
        CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection("couchbase://" + USERNAME + ":" + PASSWORD
                + "@localhost:" + couchbase.getMappedPort(8091), BUCKET_NAME);
        Assert.assertTrue(couchbaseConnection.get("artist:vincent_van_gogh").content().containsKey("notableWorks"));
    }
}
