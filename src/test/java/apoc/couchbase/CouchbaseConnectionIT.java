package apoc.couchbase;

import com.couchbase.client.java.auth.PasswordAuthenticator;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.Arrays;

import static org.junit.Assume.assumeTrue;

import static apoc.couchbase.CouchbaseTestUtils.*;

/**
 * Created by alberto.delazzari on 22/08/2018.
 */
public class CouchbaseConnectionIT {

    private static final String COUCHBASE_CONFIG_KEY = "demo";

    private static GraphDatabaseService db;

    private static int COUCHBASE_SERVER_VERSION;

    private static DefaultCouchbaseEnvironment ENV;

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
        String baseConfigKey = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COUCHBASE_CONFIG_KEY + ".";

        ENV = DefaultCouchbaseEnvironment.builder().bootstrapHttpDirectPort(couchbase.getMappedPort(8091)).build();

        COUCHBASE_SERVER_VERSION = getVersion(couchbase);
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig(baseConfigKey + CouchbaseManager.URI_CONFIG_KEY, "localhost")
                .setConfig(baseConfigKey + CouchbaseManager.USERNAME_CONFIG_KEY, USERNAME)
                .setConfig(baseConfigKey + CouchbaseManager.PASSWORD_CONFIG_KEY, PASSWORD
                )
                .newGraphDatabase();
    }

    @Test
    public void testGetCouchbaseServerVersion() {
        PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator(USERNAME, PASSWORD);
        CouchbaseConnection connection = new CouchbaseConnection(Arrays.asList("couchbase://localhost"), passwordAuthenticator, BUCKET_NAME, null, ENV);
        Assert.assertEquals(COUCHBASE_SERVER_VERSION, connection.getMajorVersion());
    }

    @Test
    public void testInsertAndRemoveDocument() {
        PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator(USERNAME, PASSWORD);
        CouchbaseConnection connection = new CouchbaseConnection(Arrays.asList("couchbase://localhost"), passwordAuthenticator, BUCKET_NAME, null, ENV);

        JsonDocument document = connection.upsert("test", JsonObject.create().put("test", "test").toString());
        Assert.assertEquals("test", document.content().get("test"));

        connection.remove("test");
        Assert.assertNull(connection.get("test"));
    }

    @Test
    public void testGetDocument() {
        PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator(USERNAME, PASSWORD);
        CouchbaseConnection connection = new CouchbaseConnection(Arrays.asList("couchbase://localhost"), passwordAuthenticator, BUCKET_NAME, null, ENV);

        JsonDocument document = connection.get("artist:vincent_van_gogh");
        Assert.assertEquals("Vincent", document.content().get("firstName"));
    }
}
