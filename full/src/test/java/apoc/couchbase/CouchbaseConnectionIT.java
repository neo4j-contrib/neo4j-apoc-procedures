package apoc.couchbase;

import apoc.util.TestUtil;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.PasswordAuthenticator;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.junit.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.Arrays;

import static apoc.ApocSettings.dynamic;
import static apoc.couchbase.CouchbaseTestUtils.*;
import static apoc.util.TestUtil.isTravis;
import static org.junit.Assume.*;
import static org.neo4j.configuration.SettingValueParsers.STRING;

/**
 * Created by alberto.delazzari on 22/08/2018.
 */
@Ignore // The same tests are covered from CouchbaseIT, for now we disable this in order to reduce the build time
public class CouchbaseConnectionIT {

    private static final String COUCHBASE_CONFIG_KEY = "demo";
    static final String baseConfigKey = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COUCHBASE_CONFIG_KEY + ".";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(dynamic(baseConfigKey + CouchbaseManager.URI_CONFIG_KEY, STRING), "localhost")
            .withSetting(dynamic(baseConfigKey + CouchbaseManager.USERNAME_CONFIG_KEY, STRING), USERNAME)
            .withSetting(dynamic(baseConfigKey + CouchbaseManager.PASSWORD_CONFIG_KEY, STRING),  PASSWORD)
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + CONNECTION_TIMEOUT_CONFIG_KEY, STRING),
                    CONNECTION_TIMEOUT_CONFIG_VALUE)
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + SOCKET_CONNECT_TIMEOUT_CONFIG_KEY, STRING),
                    SOCKET_CONNECT_TIMEOUT_CONFIG_VALUE)
            .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + KV_TIMEOUT_CONFIG_KEY, STRING),
                    KV_TIMEOUT_CONFIG_VALUE);

    private static int COUCHBASE_SERVER_VERSION;

    private CouchbaseEnvironment ENV;

    public static CouchbaseContainer couchbase;

    @BeforeClass
    public static void setUp() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            couchbase = new CouchbaseContainer()
                    .withCredentials(USERNAME, PASSWORD)
                    .withBucket(new BucketDefinition(BUCKET_NAME));
            couchbase.start();
        }, Exception.class);
        assumeNotNull(couchbase);
        assumeTrue("couchbase must be running", couchbase.isRunning());

        CouchbaseEnvironment environment = DefaultCouchbaseEnvironment
                .builder()
                .bootstrapCarrierDirectPort(couchbase.getBootstrapCarrierDirectPort())
                .bootstrapHttpDirectPort(couchbase.getBootstrapHttpDirectPort())
                .build();

        Cluster cluster = CouchbaseCluster.create(
                environment,
                couchbase.getHost()
        );

        boolean isFilled = fillDB(cluster);
        assumeTrue("should fill Couchbase with data", isFilled);
        COUCHBASE_SERVER_VERSION = getVersion(couchbase);
    }

    @AfterClass
    public static void tearDown() {
        if (couchbase != null) {
            couchbase.stop();
        }
    }

    @Before
    public void before() {
        ENV = DefaultCouchbaseEnvironment.builder().bootstrapHttpDirectPort(couchbase.getMappedPort(8091)).build();
    }

    @Test
    public void testGetCouchbaseServerVersion() {
        PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator(USERNAME, PASSWORD);
        try (CouchbaseConnection connection = new CouchbaseConnection(Arrays.asList("couchbase://localhost"), passwordAuthenticator, BUCKET_NAME, null, ENV);){
            Assert.assertEquals(COUCHBASE_SERVER_VERSION, connection.getMajorVersion());
        }
    }

    @Test
    public void testInsertAndRemoveDocument() {
        PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator(USERNAME, PASSWORD);
        try (CouchbaseConnection connection = new CouchbaseConnection(Arrays.asList("couchbase://localhost"), passwordAuthenticator, BUCKET_NAME, null, ENV);) {
            JsonDocument document = connection.upsert("test", JsonObject.create().put("test", "test").toString());
            Assert.assertEquals("test", document.content().get("test"));

            connection.remove("test");
            Assert.assertNull(connection.get("test"));
        }

    }

    @Test
    public void testGetDocument() {
        PasswordAuthenticator passwordAuthenticator = new PasswordAuthenticator(USERNAME, PASSWORD);
        try (CouchbaseConnection connection = new CouchbaseConnection(Arrays.asList("couchbase://localhost"), passwordAuthenticator, BUCKET_NAME, null, ENV);) {
            JsonDocument document = connection.get("artist:vincent_van_gogh");
            Assert.assertEquals("Vincent", document.content().get("firstName"));
        }
    }
}
