package apoc.couchbase;

import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.couchbase.CouchbaseTestUtils.BASE_CONFIG_KEY;
import static apoc.couchbase.CouchbaseTestUtils.BUCKET_NAME;
import static apoc.couchbase.CouchbaseTestUtils.COUCHBASE_CONFIG_KEY;
import static apoc.couchbase.CouchbaseTestUtils.PASSWORD;
import static apoc.couchbase.CouchbaseTestUtils.USERNAME;
import static apoc.couchbase.CouchbaseTestUtils.couchbase;
import static apoc.couchbase.CouchbaseTestUtils.createCouchbaseContainer;

/**
 * Created by alberto.delazzari on 22/08/2018.
 */
@Ignore // The same tests are covered from CouchbaseIT, for now we disable this in order to reduce the build time
public class CouchbaseConnectionIT {

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
                BASE_CONFIG_KEY + CouchbaseManager.PORT_CONFIG_KEY, couchbase.getMappedPort(8091)
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
    public void testInsertAndRemoveDocument() {
        PasswordAuthenticator passwordAuthenticator = PasswordAuthenticator.create(USERNAME, PASSWORD);
        try (CouchbaseConnection connection = new CouchbaseConnection(COUCHBASE_CONFIG_KEY, passwordAuthenticator, BUCKET_NAME, ClusterEnvironment.create())) {
            connection.upsert("test", JsonObject.create().put("test", "test").toString());
            GetResult document = connection.get("test");
            Assert.assertEquals("test", document.contentAsObject().get("test"));

            connection.remove("test");
            Assert.assertNull(connection.get("test"));
        }
    }

    @Test
    public void testGetDocument() {
        PasswordAuthenticator passwordAuthenticator = PasswordAuthenticator.create(USERNAME, PASSWORD);
        try (CouchbaseConnection connection = new CouchbaseConnection(COUCHBASE_CONFIG_KEY, passwordAuthenticator, BUCKET_NAME, ClusterEnvironment.create())) {
            GetResult document = connection.get("artist:vincent_van_gogh");
            Assert.assertEquals("Vincent", document.contentAsObject().get("firstName"));
        }
    }
}