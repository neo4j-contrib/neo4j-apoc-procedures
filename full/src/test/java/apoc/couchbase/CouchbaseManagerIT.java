package apoc.couchbase;

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
 * Created by alberto.delazzari on 23/08/2018.
 */
@Ignore // The same tests are covered from CouchbaseIT, for now we disable this in order to reduce the build time
public class CouchbaseManagerIT {

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
    public void testGetConnectionWithKey() {
        try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection(COUCHBASE_CONFIG_KEY, BUCKET_NAME);) {
            Assert.assertTrue(couchbaseConnection.get("artist:vincent_van_gogh").contentAsObject().containsKey("notableWorks"));
        }
    }

    @Test
    public void testGetConnectionWithHost() {
        try (CouchbaseConnection couchbaseConnection = CouchbaseManager.getConnection("couchbase://" + USERNAME + ":" + PASSWORD
                + "@localhost:" + couchbase.getMappedPort(8091), BUCKET_NAME);) {
            Assert.assertTrue(couchbaseConnection.get("artist:vincent_van_gogh").contentAsObject().containsKey("notableWorks"));
        }
    }
}