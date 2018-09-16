package apoc.couchbase;

import com.couchbase.client.java.auth.PasswordAuthenticator;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;

import static org.junit.Assume.assumeTrue;

/**
 * Created by alberto.delazzari on 22/08/2018.
 */
public class CouchbaseConnectionIT extends CouchbaseAbstractTest {

    private static final String COUCHBASE_CONFIG_KEY = "demo";

    @BeforeClass
    public static void setUp() throws Exception {
        CouchbaseAbstractTest.setUp();

        assumeTrue(couchbaseRunning);
        if (couchbaseRunning) {
            String baseConfigKey = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COUCHBASE_CONFIG_KEY + ".";

            new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                    .setConfig(baseConfigKey + CouchbaseManager.URI_CONFIG_KEY, "localhost")
                    .setConfig(baseConfigKey + CouchbaseManager.USERNAME_CONFIG_KEY, USERNAME)
                    .setConfig(baseConfigKey + CouchbaseManager.PASSWORD_CONFIG_KEY, PASSWORD
                    )
                    .newGraphDatabase();
        }
    }

    @AfterClass
    public static void tearDown() {
        if (couchbaseRunning) {
            couchbaseCluster.authenticate(USERNAME, PASSWORD).clusterManager().removeBucket(BUCKET_NAME);
        }
    }

    @Before
    public void assumeIsRunning() {
        assumeTrue(couchbaseRunning);
    }

    @Test
    public void testGetCouchbaseServerVersion() {
        PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator(USERNAME, PASSWORD);
        CouchbaseConnection connection = new CouchbaseConnection(Arrays.asList("couchbase://localhost"), passwordAuthenticator, BUCKET_NAME, null);
        Assert.assertEquals(COUCHBASE_SERVER_VERSION, connection.getMajorVersion());
    }

    @Test
    public void testInsertAndRemoveDocument() {
        PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator(USERNAME, PASSWORD);
        CouchbaseConnection connection = new CouchbaseConnection(Arrays.asList("couchbase://localhost"), passwordAuthenticator, BUCKET_NAME, null);

        JsonDocument document = connection.upsert("test", JsonObject.create().put("test", "test").toString());
        Assert.assertEquals("test", document.content().get("test"));

        connection.remove("test");
        Assert.assertNull(connection.get("test"));
    }

    @Test
    public void testGetDocument() {
        PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator(USERNAME, PASSWORD);
        CouchbaseConnection connection = new CouchbaseConnection(Arrays.asList("couchbase://localhost"), passwordAuthenticator, BUCKET_NAME, null);

        JsonDocument document = connection.get("artist:vincent_van_gogh");
        Assert.assertEquals("Vincent", document.content().get("firstName"));
    }
}
