package apoc.couchbase;

import com.couchbase.client.java.auth.PasswordAuthenticator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by alberto.delazzari on 24/08/2018.
 */
public class CouchbaseManagerTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    private static final String USERNAME = "Administrator";

    private static final String PASSWORD = "password";

    private static final String COUCHBASE_CONFIG_KEY = "demo";

    @BeforeClass
    public static void setUp() throws Exception {

        String baseConfigKey = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COUCHBASE_CONFIG_KEY + ".";

        new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig(baseConfigKey + CouchbaseManager.URI_CONFIG_KEY, "localhost")
                .setConfig(baseConfigKey + CouchbaseManager.USERNAME_CONFIG_KEY, USERNAME)
                .setConfig(baseConfigKey + CouchbaseManager.PASSWORD_CONFIG_KEY, PASSWORD
                )
                .newGraphDatabase();
    }

    @Test
    public void testURIFailsWithExceptionIfNotCredentials() {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("URI must include credentials otherwise use apoc.couchbase.<key>.* configuration");
        CouchbaseManager.checkAndGetURI("couchbase://localhost:11210");
    }

    @Test
    public void testURIFailsWithExceptionIfMissingCredentialsFormat() {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Credentials must be defined according URI specifications");
        CouchbaseManager.checkAndGetURI("couchbase://username@localhost:11210");
    }

    @Test
    public void testCompleteURI() {
        Tuple2<PasswordAuthenticator, List<String>> connectionObjectsFromHostOrKey = CouchbaseManager.getConnectionObjectsFromHostOrKey("couchbase://" + USERNAME + ":" + PASSWORD + "@localhost:11210");

        Assert.assertEquals(USERNAME, connectionObjectsFromHostOrKey.v1().username());
        Assert.assertEquals(PASSWORD, connectionObjectsFromHostOrKey.v1().password());
        Assert.assertEquals(Arrays.asList("couchbase://localhost:11210"), connectionObjectsFromHostOrKey.v2());
    }

    @Test
    public void testCompleteURIButNoPort() {
        Tuple2<PasswordAuthenticator, List<String>> connectionObjectsFromHostOrKey = CouchbaseManager.getConnectionObjectsFromHostOrKey("couchbase://" + USERNAME + ":" + PASSWORD + "@localhost");

        Assert.assertEquals(USERNAME, connectionObjectsFromHostOrKey.v1().username());
        Assert.assertEquals(PASSWORD, connectionObjectsFromHostOrKey.v1().password());
        Assert.assertEquals(Arrays.asList("couchbase://localhost"), connectionObjectsFromHostOrKey.v2());
    }

    @Test
    public void testIfSchemeIsNotDefineConsiderConfigurationKeyAndConfigurationKeyNotExists() {
        String hostOrKey = "localhost";

        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Please check neo4j.conf file 'apoc.couchbase." + hostOrKey + "' is missing");

        CouchbaseManager.getConnectionObjectsFromConfigurationKey(hostOrKey);
    }

    @Test
    public void testIfSchemeIsNotDefineConsiderConfigurationKeyAndConfigurationExists() {
        String hostOrKey = COUCHBASE_CONFIG_KEY;

        Tuple2<PasswordAuthenticator, List<String>> connectionObjectsFromHostOrKey = CouchbaseManager.getConnectionObjectsFromConfigurationKey(hostOrKey);

        Assert.assertEquals(USERNAME, connectionObjectsFromHostOrKey.v1().username());
        Assert.assertEquals(PASSWORD, connectionObjectsFromHostOrKey.v1().password());
        Assert.assertEquals(Arrays.asList("localhost"), connectionObjectsFromHostOrKey.v2());
    }
}
