package apoc.couchbase;

import com.couchbase.client.java.auth.PasswordAuthenticator;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.List;

import static apoc.ApocSettings.dynamic;
import static apoc.couchbase.CouchbaseTestUtils.*;
import static org.neo4j.configuration.SettingValueParsers.STRING;

/**
 * Created by alberto.delazzari on 24/08/2018.
 */
@Ignore
public class CouchbaseManagerTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    private static final String USERNAME = "Administrator";

    private static final String PASSWORD = "password";

    private static final String COUCHBASE_CONFIG_KEY = "demo";

    private static final String BASE_CONFIG_KEY = "apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + COUCHBASE_CONFIG_KEY + ".";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
                .withSetting(dynamic(BASE_CONFIG_KEY + CouchbaseManager.URI_CONFIG_KEY, STRING), "localhost")
                .withSetting(dynamic(BASE_CONFIG_KEY + CouchbaseManager.USERNAME_CONFIG_KEY, STRING), USERNAME)
                .withSetting(dynamic(BASE_CONFIG_KEY + CouchbaseManager.PASSWORD_CONFIG_KEY, STRING), PASSWORD)
                .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + CONNECTION_TIMEOUT_CONFIG_KEY, STRING),
                           CONNECTION_TIMEOUT_CONFIG_VALUE)
                .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + SOCKET_CONNECT_TIMEOUT_CONFIG_KEY, STRING),
                           SOCKET_CONNECT_TIMEOUT_CONFIG_VALUE)
                .withSetting(dynamic("apoc." + CouchbaseManager.COUCHBASE_CONFIG_KEY + KV_TIMEOUT_CONFIG_KEY, STRING),
                           KV_TIMEOUT_CONFIG_VALUE);

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

        Assert.assertEquals(USERNAME, connectionObjectsFromHostOrKey.first().username());
        Assert.assertEquals(PASSWORD, connectionObjectsFromHostOrKey.first().password());
        Assert.assertEquals(Arrays.asList("couchbase://localhost:11210"), connectionObjectsFromHostOrKey.other());
    }

    @Test
    public void testCompleteURIButNoPort() {
        Pair<PasswordAuthenticator, List<String>> connectionObjectsFromHostOrKey = CouchbaseManager.getConnectionObjectsFromHostOrKey("couchbase://" + USERNAME + ":" + PASSWORD + "@localhost");

        Assert.assertEquals(USERNAME, connectionObjectsFromHostOrKey.first().username());
        Assert.assertEquals(PASSWORD, connectionObjectsFromHostOrKey.first().password());
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

        Assert.assertEquals(USERNAME, connectionObjectsFromHostOrKey.first().username());
        Assert.assertEquals(PASSWORD, connectionObjectsFromHostOrKey.first().password());
        Assert.assertEquals(Arrays.asList("localhost"), connectionObjectsFromHostOrKey.other());
    }

    @Test
    public void testConfig() {
        Assert.assertEquals(CONNECTION_TIMEOUT_CONFIG_VALUE, CouchbaseManager.getConfig(CONNECTION_TIMEOUT_CONFIG_KEY));
    }
}
