package apoc.couchbase;

import com.couchbase.client.core.env.PasswordAuthenticator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.couchbase.CouchbaseTestUtils.*;

/**
 * Created by alberto.delazzari on 24/08/2018.
 */
@Ignore
public class CouchbaseManagerTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        createCouchbaseContainer();

        // if we have several hosts comma separated, we can connect if at least one of them is valid
        Map<String, Object> properties = Map.of(
                BASE_CONFIG_KEY + CouchbaseManager.URI_CONFIG_KEY, "invalidOne,localhost,invalidTwo",
                BASE_CONFIG_KEY + CouchbaseManager.USERNAME_CONFIG_KEY, USERNAME,
                BASE_CONFIG_KEY + CouchbaseManager.PASSWORD_CONFIG_KEY, PASSWORD,
                BASE_CONFIG_KEY + CouchbaseManager.PORT_CONFIG_KEY, couchbase.getMappedPort(8091),
                BASE_APOC_CONFIG + CONNECTION_TIMEOUT_CONFIG_KEY, CONNECTION_TIMEOUT_CONFIG_VALUE,
                BASE_APOC_CONFIG + KV_TIMEOUT_CONFIG_KEY, KV_TIMEOUT_CONFIG_VALUE,
                BASE_APOC_CONFIG + IO_POOL_SIZE_CONFIG_KEY, IO_POOL_SIZE_CONFIG_VALUE
        );
        properties.forEach((key, value) -> apocConfig().setProperty(key, value));
    }

    @AfterClass
    public static void tearDown() {
        if (couchbase != null) {
            couchbase.stop();
        }
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
        Pair<PasswordAuthenticator, List<String>> connectionObjectsFromHostOrKey = CouchbaseManager.getConnectionObjectsFromHostOrKey("couchbase://" + USERNAME + ":" + PASSWORD + "@localhost:11210");
        // I cannot assert username and password because in 3.x.x are private in PasswordAuthenticator
        Assert.assertEquals(Arrays.asList("couchbase://localhost:11210"), connectionObjectsFromHostOrKey.other());
    }

    @Test
    public void testCompleteURIButNoPort() {
        Pair<PasswordAuthenticator, List<String>> connectionObjectsFromHostOrKey = CouchbaseManager.getConnectionObjectsFromHostOrKey("couchbase://" + USERNAME + ":" + PASSWORD + "@localhost");
        // I cannot assert username and password because in 3.x.x are private in PasswordAuthenticator
        Assert.assertEquals(Arrays.asList("couchbase://localhost"), connectionObjectsFromHostOrKey.other());
    }

    @Test
    public void testIfSchemeIsNotDefineConsiderConfigurationKeyAndConfigurationKeyNotExists() {
        String hostOrKey = "localhost";

        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Please check apoc.conf file 'apoc.couchbase." + hostOrKey + "' is missing");

        CouchbaseManager.getConnectionObjectsFromConfigurationKey(hostOrKey);
    }

    @Test
    public void testIfSchemeIsNotDefineConsiderConfigurationKeyAndConfigurationExists() {
        String hostOrKey = COUCHBASE_CONFIG_KEY;

        Pair<PasswordAuthenticator, List<String>> connectionObjectsFromHostOrKey = CouchbaseManager.getConnectionObjectsFromConfigurationKey(hostOrKey);
        Assert.assertEquals(Arrays.asList("invalidOne", "localhost", "invalidTwo"), connectionObjectsFromHostOrKey.other());
    }

    @Test
    public void testConfig() {
        Assert.assertEquals(CONNECTION_TIMEOUT_CONFIG_VALUE, CouchbaseManager.getConfig(CONNECTION_TIMEOUT_CONFIG_KEY));
        Assert.assertEquals(IO_POOL_SIZE_CONFIG_VALUE, CouchbaseManager.getConfig(IO_POOL_SIZE_CONFIG_KEY));
        Assert.assertEquals(KV_TIMEOUT_CONFIG_VALUE, CouchbaseManager.getConfig(KV_TIMEOUT_CONFIG_KEY));
    }
}