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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assume.assumeTrue;

/**
 * Created by alberto.delazzari on 23/08/2018.
 */
public class CouchbaseManagerIT extends CouchbaseAbstractTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    private static final String COUCHBASE_CONFIG_KEY = "demo";

    private static final String BUCKET_NAME_WITH_PASSWORD = "mysecretbucket";

    private static final String BUCKET_PASSWORD = "drowssap";

    private static int COUCHBASE_SERVER_VERSION;

    private static void setUpCouchbase() {
        JsonObject vincentVanGogh =
                JsonObject.create()
                        .put("firstName", "Vincent")
                        .put("secondName", "Willem")
                        .put("lastName", "Van Gogh")
                        .put("notableWorks", JsonArray.from("Starry Night", "Sunflowers", "Bedroom in Arles", "Portrait of Dr Gachet", "Sorrow"));

        TestUtil.ignoreException(() -> {
            couchbaseCluster = CouchbaseCluster.create(CouchbaseManager.DEFAULT_COUCHBASE_ENVIRONMENT, "localhost");

            // It seems to take a lot sometimes to create the cluster so before perform any operation on it we just wait a little bit
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {

            }

            ClusterManager clusterManager = couchbaseCluster.authenticate(USERNAME, PASSWORD).clusterManager();
            COUCHBASE_SERVER_VERSION = clusterManager.info(1, TimeUnit.SECONDS).getMinVersion().major();

            // Create a bucket with no password!!
            clusterManager.insertBucket(DefaultBucketSettings.builder()
                    .type(BucketType.COUCHBASE)
                    .replicas(0)
                    .password("")
                    .quota(100)
                    .name(BUCKET_NAME)
                    .indexReplicas(false)
                    .enableFlush(true)
                    .build()
            );

            // Check if we are running this test against Couchbase Server 4.x or 5.x
            if (COUCHBASE_SERVER_VERSION == 4) {
                // Create a bucket with password!!
                clusterManager.insertBucket(DefaultBucketSettings.builder()
                        .type(BucketType.COUCHBASE)
                        .replicas(0)
                        .password(BUCKET_PASSWORD)
                        .quota(100)
                        .name(BUCKET_NAME_WITH_PASSWORD)
                        .indexReplicas(false)
                        .enableFlush(true)
                        .build()
                );

                couchbaseCluster.disconnect();
                couchbaseCluster = CouchbaseCluster.create(CouchbaseManager.DEFAULT_COUCHBASE_ENVIRONMENT, "localhost");
            }

            Bucket couchbaseBucket = couchbaseCluster.openBucket(BUCKET_NAME);
            couchbaseBucket.upsert(JsonDocument.create("artist:vincent_van_gogh", vincentVanGogh));
            couchbaseBucket.bucketManager().createN1qlPrimaryIndex(true, false);

            // Test if it was inserted/updated correctly
            N1qlQueryResult queryResult = couchbaseBucket.query(N1qlQuery.simple("select * from " + BUCKET_NAME + " where lastName = 'Van Gogh'", N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)));
            couchbaseRunning = queryResult.info().resultCount() == 1;

            // if server version is 4, we also create a bucket with a password
            if (COUCHBASE_SERVER_VERSION == 4) {
                Bucket couchbaseBucketWithPassword = couchbaseCluster.openBucket(BUCKET_NAME_WITH_PASSWORD, BUCKET_PASSWORD);
                couchbaseBucketWithPassword.upsert(JsonDocument.create("artist:vincent_van_gogh", vincentVanGogh));
                couchbaseBucketWithPassword.bucketManager().createN1qlPrimaryIndex(true, false);
            }

        }, TimeoutException.class, CouchbaseException.class);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        setUpCouchbase();

        if (couchbaseRunning) {
            String baseConfigKey = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COUCHBASE_CONFIG_KEY + ".";
/*
            new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                    .setConfig(baseConfigKey + CouchbaseManager.URI_CONFIG_KEY, "localhost")
                    .setConfig(baseConfigKey + CouchbaseManager.USERNAME_CONFIG_KEY, USERNAME)
                    .setConfig(baseConfigKey + CouchbaseManager.PASSWORD_CONFIG_KEY, PASSWORD
                    )
                    .newGraphDatabase();
*/
        }
    }

    @AfterClass
    public static void tearDown() {
        if (couchbaseRunning) {
            couchbaseCluster.authenticate(USERNAME, PASSWORD).clusterManager().removeBucket(BUCKET_NAME);
            // Only for Couchbase Server 4.x we also remove the bucket with password
            if (COUCHBASE_SERVER_VERSION == 4) {
                couchbaseCluster.authenticate(USERNAME, PASSWORD).clusterManager().removeBucket(BUCKET_NAME_WITH_PASSWORD);
            }
        }
    }

    @Before
    public void assumeIsRunning() {
        assumeTrue(couchbaseRunning);
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
        CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection("couchbase://" + USERNAME + ":" + PASSWORD + "@localhost", BUCKET_NAME);
        Assert.assertTrue(couchbaseConnection.get("artist:vincent_van_gogh").content().containsKey("notableWorks"));
    }
}
