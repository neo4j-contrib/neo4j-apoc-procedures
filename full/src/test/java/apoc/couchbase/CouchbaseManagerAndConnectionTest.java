package apoc.couchbase;

import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import apoc.result.BooleanResult;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.ExpectedException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.couchbase.CouchbaseTestUtils.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class CouchbaseManagerAndConnectionTest {

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

    // -- tests from CouchbaseManagerTest
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
        Assert.assertEquals(Arrays.asList("couchbase://localhost:11210"), connectionObjectsFromHostOrKey.other());
    }

    @Test
    public void testCompleteURIButNoPort() {
        Pair<PasswordAuthenticator, List<String>> connectionObjectsFromHostOrKey = CouchbaseManager.getConnectionObjectsFromHostOrKey("couchbase://" + USERNAME + ":" + PASSWORD + "@localhost");
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


    // -- tests from CouchbaseConnectionIT
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

    // -- tests from CouchbaseManagerIT
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

    // -- tests from CouchbaseTest
    @Test
    @SuppressWarnings("unchecked")
    public void testGet() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseJsonDocument> stream = new Couchbase().get(HOST, BUCKET_NAME, "artist:vincent_van_gogh");
            Iterator<CouchbaseJsonDocument> iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            CouchbaseJsonDocument couchbaseJsonDocument = iterator.next();
            checkDocumentContent(
                    (String) couchbaseJsonDocument.getContent().get("firstName"),
                    (String) couchbaseJsonDocument.getContent().get("secondName"),
                    (String) couchbaseJsonDocument.getContent().get("lastName"),
                    (List<String>) couchbaseJsonDocument.getContent().get("notableWorks"));
            assertFalse(iterator.hasNext());
            //@eclipse-formatter:on
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testExists() {
        try {
            //@eclipse-formatter:off
            Stream<BooleanResult> stream = new Couchbase().exists(HOST, BUCKET_NAME, "artist:vincent_van_gogh");
            //@eclipse-formatter:on
            Iterator<BooleanResult> iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            BooleanResult booleanResult = iterator.next();
            assertTrue(booleanResult.value);
            assertFalse(iterator.hasNext());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsert() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseJsonDocument> stream = new Couchbase().insert(HOST, BUCKET_NAME, "testInsert", VINCENT_VAN_GOGH.toString());
            Iterator<CouchbaseJsonDocument> iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            CouchbaseJsonDocument couchbaseJsonDocument = iterator.next();
            checkDocumentContent(
                    (String) couchbaseJsonDocument.getContent().get("firstName"),
                    (String) couchbaseJsonDocument.getContent().get("secondName"),
                    (String) couchbaseJsonDocument.getContent().get("lastName"),
                    (List<String>) couchbaseJsonDocument.getContent().get("notableWorks"));
            assertFalse(iterator.hasNext());
            //@eclipse-formatter:on
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            collection.remove("testInsert");
            assertFalse(collection.exists("testInsert").exists());
        }
    }

    @Test(expected = DocumentExistsException.class)
    public void testInsertWithAlreadyExistingID() {
        //@eclipse-formatter:off
        new Couchbase().insert(HOST, BUCKET_NAME, "artist:vincent_van_gogh", "{}");
        //@eclipse-formatter:on
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpsert() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseJsonDocument> stream = new Couchbase().upsert(HOST, BUCKET_NAME, "testUpsert", VINCENT_VAN_GOGH.toString());
            Iterator<CouchbaseJsonDocument> iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            CouchbaseJsonDocument couchbaseJsonDocument = iterator.next();
            checkDocumentContent(
                    (String) couchbaseJsonDocument.getContent().get("firstName"),
                    (String) couchbaseJsonDocument.getContent().get("secondName"),
                    (String) couchbaseJsonDocument.getContent().get("lastName"),
                    (List<String>) couchbaseJsonDocument.getContent().get("notableWorks"));
            assertFalse(iterator.hasNext());
            //@eclipse-formatter:on
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            collection.remove("testUpsert");
            assertFalse(collection.exists("testUpsert").exists());
        }
    }

    @Test
    public void testRemove() {
        try {
            //@eclipse-formatter:off
            collection.insert("testRemove", "");
            new Couchbase().remove(HOST, BUCKET_NAME, "testRemove");
            assertFalse(collection.exists("testRemove").exists());
            //@eclipse-formatter:on
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testQuery() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseQueryResult> stream = new Couchbase().query(HOST, BUCKET_NAME, "select * from " + BUCKET_NAME + " where lastName = 'Van Gogh'");
            //@eclipse-formatter:on
            checkQueryResult(stream);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testQueryWithPositionalParams() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseQueryResult> stream = new Couchbase().posParamsQuery(HOST, BUCKET_NAME, "select * from " + BUCKET_NAME + " where lastName = $1", Arrays.asList("Van Gogh"));
            //@eclipse-formatter:on
            checkQueryResult(stream);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testQueryWithNamedParams() {
        try {
            //@eclipse-formatter:off
            Stream<CouchbaseQueryResult> stream = new Couchbase().namedParamsQuery(HOST, BUCKET_NAME, "select * from " + BUCKET_NAME + " where lastName = $lastName", Arrays.asList("lastName"), Arrays.asList("Van Gogh"));
            //@eclipse-formatter:on
            checkQueryResult(stream);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
